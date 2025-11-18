open Oauth

let options_handler = Xrpc.handler (fun _ -> Dream.empty `No_Content)

let post_handler =
  Xrpc.handler ~auth:DPoP (fun ctx ->
      let%lwt req = Xrpc.parse_body ctx.req Types.token_request_of_yojson in
      let proof = Auth.get_dpop_proof_exn ctx.auth in
      let ip = Dream.client ctx.req in
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
                if Util.now_ms () > code_rec.expires_at then
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
                      | Some par_record ->
                          let orig_req =
                            Yojson.Safe.from_string par_record.request_data
                            |> Types.par_request_of_yojson |> Result.get_ok
                          in
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
                          let token_id =
                            "tok-"
                            ^ Uuidm.to_string
                                (Uuidm.v4_gen
                                   (Random.State.make_self_init ())
                                   () )
                          in
                          let refresh_token =
                            "ref-"
                            ^ Uuidm.to_string
                                (Uuidm.v4_gen
                                   (Random.State.make_self_init ())
                                   () )
                          in
                          let now_sec = int_of_float (Unix.gettimeofday ()) in
                          let now_ms = Util.now_ms () in
                          let expires_in =
                            Constants.access_token_expiry_ms / 1000
                          in
                          let exp_sec = now_sec + expires_in in
                          let expires_at = exp_sec * 1000 in
                          let claims =
                            `Assoc
                              [ ("jti", `String token_id)
                              ; ("sub", `String did)
                              ; ("iat", `Int now_sec)
                              ; ("exp", `Int exp_sec)
                              ; ("scope", `String orig_req.scope)
                              ; ("aud", `String ("https://" ^ Env.hostname))
                              ; ("cnf", `Assoc [("jkt", `String proof.jkt)]) ]
                          in
                          let access_token =
                            Jwt.sign_jwt claims ~typ:"at+jwt" Env.jwt_key
                          in
                          let%lwt () =
                            Queries.insert_oauth_token ctx.db
                              { refresh_token
                              ; client_id= req.client_id
                              ; did
                              ; dpop_jkt= proof.jkt
                              ; scope= orig_req.scope
                              ; created_at= now_ms
                              ; last_refreshed_at= now_ms
                              ; expires_at
                              ; last_ip= ip
                              ; last_user_agent= user_agent }
                          in
                          let nonce = Dpop.next_nonce () in
                          Dream.json
                            ~headers:
                              [ ("DPoP-Nonce", nonce)
                              ; ("Access-Control-Expose-Headers", "DPoP-Nonce")
                              ; ("Cache-Control", "no-store") ]
                          @@ Yojson.Safe.to_string
                          @@ `Assoc
                               [ ("access_token", `String access_token)
                               ; ("token_type", `String "DPoP")
                               ; ("refresh_token", `String refresh_token)
                               ; ("expires_in", `Int expires_in)
                               ; ("scope", `String orig_req.scope)
                               ; ("sub", `String did) ] ) ) ) )
      | "refresh_token" -> (
        match req.refresh_token with
        | None ->
            Errors.invalid_request "refresh_token required"
        | Some refresh_token -> (
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
                else
                  let new_token_id =
                    "tok-"
                    ^ Uuidm.to_string
                        (Uuidm.v4_gen (Random.State.make_self_init ()) ())
                  in
                  let new_refresh =
                    "ref-"
                    ^ Uuidm.to_string
                        (Uuidm.v4_gen (Random.State.make_self_init ()) ())
                  in
                  let now_sec = int_of_float (Unix.gettimeofday ()) in
                  let expires_in = Constants.access_token_expiry_ms / 1000 in
                  let exp_sec = now_sec + expires_in in
                  let new_expires_at = exp_sec * 1000 in
                  let claims =
                    `Assoc
                      [ ("jti", `String new_token_id)
                      ; ("sub", `String session.did)
                      ; ("iat", `Int now_sec)
                      ; ("exp", `Int exp_sec)
                      ; ("scope", `String session.scope)
                      ; ("aud", `String ("https://" ^ Env.hostname))
                      ; ("cnf", `Assoc [("jkt", `String proof.jkt)]) ]
                  in
                  let new_access_token =
                    Jwt.sign_jwt claims ~typ:"at+jwt" Env.jwt_key
                  in
                  let%lwt () =
                    Queries.update_oauth_token ctx.db
                      ~old_refresh_token:refresh_token
                      ~new_refresh_token:new_refresh ~expires_at:new_expires_at
                  in
                  Dream.json ~headers:[("Cache-Control", "no-store")]
                  @@ Yojson.Safe.to_string
                  @@ `Assoc
                       [ ("access_token", `String new_access_token)
                       ; ("token_type", `String "DPoP")
                       ; ("refresh_token", `String new_refresh)
                       ; ("expires_in", `Int expires_in)
                       ; ("scope", `String session.scope)
                       ; ("sub", `String session.did) ] ) )
      | _ ->
          Errors.invalid_request ("unsupported grant_type: " ^ req.grant_type) )
