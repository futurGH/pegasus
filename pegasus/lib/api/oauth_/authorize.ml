open Oauth
open Oauth.Types

let get_handler =
  Xrpc.handler (fun ctx ->
      let login_redirect =
        Uri.make ~path:"/account/login" ~query:(Util.copy_query ctx.req) ()
        |> Uri.to_string |> Dream.redirect ctx.req
      in
      let client_id = Dream.query ctx.req "client_id" in
      let request_uri = Dream.query ctx.req "request_uri" in
      match (client_id, request_uri) with
      | None, _ | _, None ->
          login_redirect
      | Some client_id, Some request_uri -> (
          let prefix = Constants.request_uri_prefix in
          if not (String.starts_with ~prefix request_uri) then login_redirect
          else
            let request_id =
              String.sub request_uri (String.length prefix)
                (String.length request_uri - String.length prefix)
            in
            match%lwt Queries.get_par_request ctx.db request_id with
            | None ->
                login_redirect
            | Some req_record -> (
                if req_record.client_id <> client_id then login_redirect
                else
                  let req =
                    Yojson.Safe.from_string req_record.request_data
                    |> par_request_of_yojson
                    |> Result.map_error (fun _ ->
                        Errors.internal_error ~msg:"failed to parse par request"
                          () )
                    |> Result.get_ok
                  in
                  let%lwt metadata =
                    try%lwt Client.fetch_client_metadata client_id
                    with _ ->
                      Errors.internal_error
                        ~msg:"failed to fetch client metadata" ()
                  in
                  let code =
                    "cod-"
                    ^ Uuidm.to_string
                        (Uuidm.v4_gen (Random.State.make_self_init ()) ())
                  in
                  let expires_at = Util.now_ms () + Constants.code_expiry_ms in
                  let%lwt () =
                    Queries.insert_auth_code ctx.db
                      { code
                      ; request_id
                      ; authorized_by= None
                      ; authorized_at= None
                      ; expires_at
                      ; used= false }
                  in
                  match%lwt Session.Raw.get_session ctx.req with
                  | None ->
                      login_redirect
                  | Some session when session.logged_in_dids = [] ->
                      login_redirect
                  | Some {current_did; logged_in_dids} -> (
                      let%lwt did =
                        match req.login_hint with
                        | Some hint when List.mem hint logged_in_dids ->
                            let%lwt () =
                              if Some hint <> current_did then
                                Session.Raw.set_current_did ctx.req hint
                              else Lwt.return_unit
                            in
                            Lwt.return_some hint
                        | _ ->
                            Lwt.return current_did
                      in
                      match did with
                      | None ->
                          login_redirect
                      | Some did ->
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
                          let csrf_token = Dream.csrf_token ctx.req in
                          let client_id_uri =
                            Uri.of_string metadata.client_id
                          in
                          let host, path =
                            ( Uri.host_with_default client_id_uri
                                ~default:"unknown"
                            , Uri.path client_id_uri )
                          in
                          let client_url = (host, path) in
                          let client_name = metadata.client_name in
                          Util.render_html ~title:("Authorizing " ^ host)
                            (module Frontend.OauthAuthorizePage)
                            ~props:
                              { client_url
                              ; client_name
                              ; handle
                              ; scopes
                              ; code
                              ; request_uri
                              ; csrf_token } ) ) ) )

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Errors.auth_required "missing authentication"
      | Some user_did -> (
        match%lwt Dream.form ctx.req with
        | `Ok fields -> (
            let action = List.assoc_opt "action" fields in
            let code = List.assoc_opt "code" fields in
            let request_uri = List.assoc_opt "request_uri" fields in
            match (action, code, request_uri) with
            | Some "deny", _, Some request_uri -> (
                let prefix = Constants.request_uri_prefix in
                let request_id =
                  String.sub request_uri (String.length prefix)
                    (String.length request_uri - String.length prefix)
                in
                let%lwt req_record =
                  Queries.get_par_request ctx.db request_id
                in
                match req_record with
                | Some rec_ ->
                    let req =
                      Yojson.Safe.from_string rec_.request_data
                      |> par_request_of_yojson |> Result.get_ok
                    in
                    let params =
                      [ ("error", "access_denied")
                      ; ("error_description", "Unable to authorize user.")
                      ; ("state", req.state)
                      ; ("iss", "https://" ^ Env.hostname) ]
                    in
                    let query =
                      String.concat "&"
                        (List.map
                           (fun (k, v) -> k ^ "=" ^ Uri.pct_encode v)
                           params )
                    in
                    Dream.redirect ctx.req (req.redirect_uri ^ "?" ^ query)
                | None ->
                    Errors.invalid_request "request expired" )
            | Some "allow", Some code, Some _request_uri -> (
                let%lwt code_record = Queries.get_auth_code ctx.db code in
                match code_record with
                | None ->
                    Errors.invalid_request "invalid code"
                | Some code_rec -> (
                    if code_rec.authorized_by <> None then
                      Errors.invalid_request "code already authorized"
                    else if code_rec.used then
                      Errors.invalid_request "code already used"
                    else if Util.now_ms () > code_rec.expires_at then
                      Errors.invalid_request "code expired"
                    else
                      let%lwt () =
                        Queries.activate_auth_code ctx.db code user_did
                      in
                      let%lwt req_record =
                        Queries.get_par_request ctx.db code_rec.request_id
                      in
                      match req_record with
                      | None ->
                          Errors.internal_error ~msg:"request not found" ()
                      | Some rec_ ->
                          let req =
                            Yojson.Safe.from_string rec_.request_data
                            |> par_request_of_yojson |> Result.get_ok
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
                            | Some "fragment" ->
                                "#"
                            | _ ->
                                "?"
                          in
                          Dream.redirect ctx.req
                            (req.redirect_uri ^ separator ^ query) ) )
            | _ ->
                Errors.invalid_request "invalid request" )
        | _ ->
            Errors.invalid_request "invalid request" ) )
