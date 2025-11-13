open Oauth.Types

let get_session_user (ctx : Xrpc.context) =
  match Dream.session_field ctx.req "did" with
  | Some did ->
      Lwt.return_some did
  | None ->
      Lwt.return_none

let get_handler =
  Xrpc.handler (fun ctx ->
      let return_url = Uri.pct_encode (Dream.target ctx.req) in
      let client_id = Dream.query ctx.req "client_id" in
      let request_uri = Dream.query ctx.req "request_uri" in
      match (client_id, request_uri) with
      | None, _ | _, None ->
          (* TODO: actually implement the page for this redirect *)
          Dream.redirect ctx.req ("/login?return_to=" ^ return_url)
      | Some client_id, Some request_uri -> (
          let prefix = Oauth.Constants.request_uri_prefix in
          if not (String.starts_with ~prefix request_uri) then
            Dream.redirect ctx.req ("/login?return_to=" ^ return_url)
          else
            let request_id =
              String.sub request_uri (String.length prefix)
                (String.length request_uri - String.length prefix)
            in
            match%lwt Oauth.Queries.get_par_request ctx.db request_id with
            | None ->
                Dream.redirect ctx.req ("/login?return_to=" ^ return_url)
            | Some req_record -> (
                if req_record.client_id <> client_id then
                  Dream.redirect ctx.req ("/login?return_to=" ^ return_url)
                else
                  let req =
                    Yojson.Safe.from_string req_record.request_data
                    |> par_request_of_yojson
                    |> Result.map_error (fun _ ->
                           Errors.internal_error
                             ~msg:"failed to parse par request" () )
                    |> Result.get_ok
                  in
                  let%lwt client =
                    try%lwt Oauth.Client.fetch_client_metadata client_id
                    with _ ->
                      Errors.internal_error
                        ~msg:"failed to fetch client metadata" ()
                  in
                  match%lwt get_session_user ctx with
                  | None ->
                      Dream.redirect ctx.req ("/login?return_to=" ^ return_url)
                  | Some did -> (
                    match req.login_hint with
                    | Some hint when hint <> did ->
                        Dream.redirect ctx.req ("/login?return_to=" ^ return_url)
                    | _ ->
                        let%lwt handle =
                          match%lwt
                            Data_store.get_actor_by_identifier did ctx.db
                          with
                          | Some {handle; _} ->
                              Lwt.return handle
                          | None ->
                              Errors.internal_error
                                ~msg:"failed to resolve user" ()
                        in
                        let scopes = String.split_on_char ' ' req.scope in
                        let client_name =
                          match client.client_name with
                          | Some name ->
                              name
                          | None ->
                              client_id
                        in
                        (* TODO: render authz page with client_name, handle, scopes, request_uri *)
                        Dream.html "" ) ) ) )

let post_handler pool =
  Xrpc.handler (fun ctx ->
      match%lwt get_session_user ctx with
      | None ->
          Errors.auth_required "missing authentication"
      | Some user_did -> (
          match%lwt Dream.form ctx.req with
          | `Ok fields -> (
              let action = List.assoc_opt "action" fields in
              let request_uri = List.assoc_opt "request_uri" fields in
              if action <> Some "allow" || request_uri = None then
                Errors.invalid_request "invalid request" ;
              let request_uri = Option.get request_uri in
              let prefix = Oauth.Constants.request_uri_prefix in
              let request_id =
                String.sub request_uri (String.length prefix)
                  (String.length request_uri - String.length prefix)
              in
              let%lwt stored_request =
                Oauth.Queries.get_par_request pool request_id
              in
              match stored_request with
              | None ->
                  Errors.invalid_request "request expired"
              | Some req_record ->
                  let req =
                    Yojson.Safe.from_string req_record.request_data
                    |> par_request_of_yojson
                    |> Result.map_error (fun _ ->
                           Errors.internal_error
                             ~msg:"failed to parse stored request" () )
                    |> Result.get_ok
                  in
                  if Util.now_ms () > req_record.expires_at then
                    Errors.invalid_request "request expired"
                  else
                    let code =
                      "cod-"
                      ^ Uuidm.to_string (Uuidm.v4_gen (Random.get_state ()) ())
                    in
                    let expires_at =
                      Util.now_ms () + Oauth.Constants.code_expiry_ms
                    in
                    let%lwt () =
                      Oauth.Queries.insert_auth_code pool
                        { code
                        ; request_id
                        ; authorized_by= Some user_did
                        ; authorized_at= Some (Util.now_ms ())
                        ; expires_at
                        ; used= false }
                    in
                    let params =
                      [ ("code", code)
                      ; ("state", req.state)
                      ; ("iss", "https://" ^ Env.hostname) ]
                    in
                    let query =
                      String.concat "&"
                        (List.map
                           (fun (k, v) -> k ^ "=" ^ Uri.pct_encode v)
                           params )
                    in
                    let separator =
                      match req.response_mode with
                      | Some "query" ->
                          "?"
                      | Some "fragment" ->
                          "#"
                      | _ ->
                          "?"
                    in
                    let redirect_uri = req.redirect_uri ^ separator ^ query in
                    Dream.redirect ctx.req redirect_uri )
          | _ ->
              Errors.invalid_request "invalid request" ) )
