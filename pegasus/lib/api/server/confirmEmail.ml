type request = {email: string; token: string} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      Auth.assert_account_scope auth ~attr:Oauth.Scopes.Email
        ~action:Oauth.Scopes.Manage ;
      let did = Auth.get_authed_did_exn auth in
      let%lwt {email; token} = Xrpc.parse_body req request_of_yojson in
      let email = String.lowercase_ascii email in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.invalid_request ~name:"AccountNotFound" "account not found"
      | Some actor ->
          if String.lowercase_ascii actor.email <> email then
            Errors.invalid_request ~name:"InvalidEmail" "email does not match"
          else (
            match (actor.auth_code, actor.auth_code_expires_at) with
            | Some auth_code, Some expires_at
              when String.starts_with ~prefix:"eml-" auth_code
                   && auth_code = token
                   && Util.now_ms () < expires_at ->
                let%lwt () = Data_store.confirm_email ~did db in
                Dream.log "email confirmed for %s" did ;
                Dream.empty `OK
            | Some _, Some expires_at when Util.now_ms () >= expires_at ->
                Errors.invalid_request ~name:"ExpiredToken" "token expired"
            | _ ->
                Errors.invalid_request ~name:"InvalidToken" "invalid token" ) )
