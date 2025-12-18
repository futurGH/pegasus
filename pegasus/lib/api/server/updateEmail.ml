type request =
  { email: string
  ; email_auth_factor: bool option [@key "emailAuthFactor"] [@default None]
  ; token: string option [@default None] }
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      Auth.assert_account_scope auth ~attr:Oauth.Scopes.Email
        ~action:Oauth.Scopes.Manage ;
      let did = Auth.get_authed_did_exn auth in
      let%lwt {email; token; _} = Xrpc.parse_body req request_of_yojson in
      let email = String.lowercase_ascii email in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor -> (
        match actor.email_confirmed_at with
        | Some _ -> (
          (* email is confirmed, require valid token *)
          match token with
          | None ->
              Errors.invalid_request ~name:"TokenRequired"
                "confirmation token required"
          | Some token -> (
            match (actor.auth_code, actor.auth_code_expires_at) with
            | Some auth_code, Some expires_at
              when String.starts_with ~prefix:"eml-" auth_code
                   && auth_code = token
                   && Util.now_ms () < expires_at ->
                let%lwt () = Data_store.update_email ~did ~email db in
                Dream.log "email updated for %s to %s" did email ;
                Dream.empty `OK
            | Some _, Some expires_at when Util.now_ms () >= expires_at ->
                Errors.invalid_request ~name:"ExpiredToken" "token expired"
            | _ ->
                Errors.invalid_request ~name:"InvalidToken" "invalid token" ) )
        | None ->
            (* email not confirmed, no token required *)
            let%lwt () = Data_store.update_email ~did ~email db in
            Dream.log "email updated for %s to %s" did email ;
            Dream.empty `OK ) )
