open Oauth
module Oauth_token = Oauth.Token

let post_handler =
  Xrpc.handler ~auth:DPoP (fun ctx ->
      let%lwt req = Xrpc.parse_body ctx.req Types.token_request_of_yojson in
      let proof = Auth.get_dpop_proof_exn ctx.auth in
      let ip = Util.request_ip ctx.req in
      let user_agent = Dream.header ctx.req "User-Agent" in
      match req.grant_type with
      | "authorization_code" -> (
        match req.code with
        | None ->
            Errors.invalid_request "code required"
        | Some code -> (
            let%lwt code_record = Queries.consume_auth_code ctx.db code in
            match code_record with
            | None ->
                Errors.invalid_request "invalid code"
            | Some code_rec -> (
                if Util.Time.now_ms () > code_rec.expires_at then
                  Errors.invalid_request "code expired"
                else
                  match code_rec.authorized_by with
                  | None ->
                      Errors.invalid_request "code not authorized"
                  | Some did -> (
                      let%lwt par_req =
                        Queries.get_par_request ctx.db code_rec.request_id
                      in
                      match par_req with
                      | None ->
                          Errors.internal_error ~msg:"request not found" ()
                      | Some par_record -> (
                        match
                          Yojson.Safe.from_string par_record.request_data
                          |> Types.par_request_of_yojson
                        with
                        | Error _ ->
                            Errors.invalid_request
                              "stored par request formatted incorrectly"
                        | Ok orig_req ->
                            ( match req.redirect_uri with
                            | None ->
                                Errors.invalid_request "redirect_uri required"
                            | Some uri when uri <> orig_req.redirect_uri ->
                                Errors.invalid_request "redirect_uri mismatch"
                            | _ ->
                                () ) ;
                            ( match req.code_verifier with
                            | None ->
                                Errors.invalid_request "code_verifier required"
                            | Some verifier ->
                                let computed =
                                  Digestif.SHA256.digest_string verifier
                                  |> Digestif.SHA256.to_raw_string
                                  |> Base64.(
                                       encode_exn ~pad:false
                                         ~alphabet:uri_safe_alphabet )
                                in
                                if orig_req.code_challenge <> computed then
                                  Errors.invalid_request "invalid code_verifier"
                            ) ;
                            ( match par_record.dpop_jkt with
                            | Some stored when stored <> proof.jkt ->
                                Errors.invalid_request "DPoP key mismatch"
                            | _ ->
                                () ) ;
                            let now_ms = Util.Time.now_ms () in
                            let now_sec = now_ms / 1000 in
                            (* expand scopes before creating token *)
                            let%lwt expanded_scopes =
                              let parsed = Scopes.parse_scopes orig_req.scope in
                              let%lwt expanded = Scopes.expand_scopes parsed in
                              Lwt.return (Scopes.scopes_to_string expanded)
                            in
                            let access_token, refresh_token =
                              Oauth_token.generate_tokens ~did
                                ~scope:expanded_scopes ~jkt:proof.jkt
                                ~now:now_sec ()
                            in
                            let auth_ip =
                              Option.value code_rec.authorized_ip ~default:ip
                            in
                            let auth_user_agent =
                              match code_rec.authorized_user_agent with
                              | Some ua ->
                                  Some ua
                              | None ->
                                  user_agent
                            in
                            let%lwt () =
                              Queries.insert_oauth_token ctx.db
                                { refresh_token= refresh_token.token
                                ; client_id= req.client_id
                                ; did
                                ; dpop_jkt= proof.jkt
                                ; scope= expanded_scopes
                                ; created_at= now_ms
                                ; last_refreshed_at= now_ms
                                ; session_expires_at=
                                    Oauth_token.session_expires_at_ms now_ms
                                ; expires_at= access_token.expires_at
                                ; last_ip= auth_ip
                                ; last_user_agent= auth_user_agent }
                            in
                            let nonce = Dpop.next_nonce () in
                            Dream.json
                              ~headers:
                                [ ("DPoP-Nonce", nonce)
                                ; ("Cache-Control", "no-store") ]
                            @@ Yojson.Safe.to_string
                            @@ `Assoc
                                 [ ("access_token", `String access_token.token)
                                 ; ("token_type", `String "DPoP")
                                 ; ("refresh_token", `String refresh_token.token)
                                 ; ("expires_in", `Int access_token.expires_in)
                                 ; ("scope", `String expanded_scopes)
                                 ; ("sub", `String did) ] ) ) ) ) )
      | "refresh_token" -> (
        match req.refresh_token with
        | None ->
            Errors.invalid_request "refresh_token required"
        | Some refresh_token -> (
            let now_ms = Util.Time.now_ms () in
            let now_sec = now_ms / 1000 in
            match
              Oauth_token.verify_refresh_token ~now:now_sec refresh_token
            with
            | Error Oauth_token.Expired ->
                Errors.invalid_request "expired refresh token"
            | Error (Oauth_token.Invalid _) ->
                Errors.invalid_request "invalid refresh token"
            | Ok verified_refresh -> (
                let%lwt token_record =
                  Queries.get_oauth_token_by_refresh ctx.db refresh_token
                in
                match token_record with
                | None ->
                    Errors.invalid_request "invalid refresh token"
                | Some session ->
                    if session.client_id <> req.client_id then
                      Errors.invalid_request "client_id mismatch"
                    else if session.dpop_jkt <> proof.jkt then
                      Errors.invalid_request "DPoP key mismatch"
                    else if verified_refresh.sub <> session.did then
                      Errors.invalid_request "refresh token subject mismatch"
                    else if now_ms >= session.session_expires_at then
                      Errors.invalid_request "session expired"
                    else
                      let access_token, new_refresh =
                        Oauth_token.generate_tokens ~did:session.did
                          ~scope:session.scope ~jkt:proof.jkt ~now:now_sec ()
                      in
                      let%lwt () =
                        Queries.update_oauth_token ctx.db
                          ~old_refresh_token:refresh_token
                          ~new_refresh_token:new_refresh.token
                          ~expires_at:access_token.expires_at
                      in
                      Dream.json ~headers:[("Cache-Control", "no-store")]
                      @@ Yojson.Safe.to_string
                      @@ `Assoc
                           [ ("access_token", `String access_token.token)
                           ; ("token_type", `String "DPoP")
                           ; ("refresh_token", `String new_refresh.token)
                           ; ("expires_in", `Int access_token.expires_in)
                           ; ("scope", `String session.scope)
                           ; ("sub", `String session.did) ] ) ) )
      | _ ->
          Errors.invalid_request ("unsupported grant_type: " ^ req.grant_type) )
