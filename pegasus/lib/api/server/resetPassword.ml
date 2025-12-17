type request = {token: string; password: string}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun {req; db; _} ->
      let%lwt {token; password} = Xrpc.parse_body req request_of_yojson in
      match%lwt Data_store.get_actor_by_auth_code ~code:token db with
      | None ->
          Errors.invalid_request ~name:"InvalidToken" "invalid or expired token"
      | Some actor -> (
        match (actor.auth_code, actor.auth_code_expires_at) with
        | Some auth_code, Some auth_expires_at
          when String.starts_with ~prefix:"pwd-" auth_code
               && token = auth_code
               && Util.now_ms () < auth_expires_at ->
            let%lwt () =
              Data_store.update_password ~did:actor.did ~password db
            in
            Dream.log "password reset completed for %s" actor.did ;
            Dream.empty `OK
        | _ ->
            Errors.invalid_request ~name:"ExpiredToken"
              "token expired or invalid" ) )
