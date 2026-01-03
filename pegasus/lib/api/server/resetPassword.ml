open Lexicons.Com.Atproto.Server.ResetPassword.Main

type reset_password_error = InvalidToken | ExpiredToken

let reset_password ~token ~password db =
  match%lwt Data_store.get_actor_by_auth_code ~code:token db with
  | None ->
      Lwt.return_error InvalidToken
  | Some actor -> (
    match (actor.auth_code, actor.auth_code_expires_at) with
    | Some auth_code, Some auth_expires_at
      when token = auth_code && Util.now_ms () < auth_expires_at ->
        let%lwt () = Data_store.update_password ~did:actor.did ~password db in
        Lwt.return_ok actor.did
    | _ ->
        Lwt.return_error ExpiredToken )

let handler =
  Xrpc.handler
    ~rate_limits:
      [ Route
          { duration_ms= 5 * Util.minute
          ; points= 50
          ; calc_key= None
          ; calc_points= None } ]
    (fun {req; db; _} ->
      let%lwt {token; password} = Xrpc.parse_body req input_of_yojson in
      match%lwt reset_password ~token ~password db with
      | Ok _ ->
          Dream.empty `OK
      | Error InvalidToken ->
          Errors.invalid_request ~name:"InvalidToken" "invalid or expired token"
      | Error ExpiredToken ->
          Errors.invalid_request ~name:"ExpiredToken" "token expired or invalid" )
