open Lexicons.Com.Atproto.Server.ConfirmEmail.Main

type confirm_error = InvalidToken | ExpiredToken | EmailMismatch

let confirm_email ~email ~token (actor : Data_store.Types.actor) db =
  let email = String.lowercase_ascii email in
  if String.lowercase_ascii actor.email <> email then
    Lwt.return_error EmailMismatch
  else
    match (actor.auth_code, actor.auth_code_expires_at) with
    | Some auth_code, Some expires_at
      when auth_code = token && Util.Time.now_ms () < expires_at ->
        let%lwt () = Data_store.confirm_email ~did:actor.did db in
        Lwt.return_ok ()
    | Some _, Some expires_at when Util.Time.now_ms () >= expires_at ->
        Lwt.return_error ExpiredToken
    | _ ->
        Lwt.return_error InvalidToken

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      Auth.assert_account_scope auth ~attr:Oauth.Scopes.Email
        ~action:Oauth.Scopes.Manage ;
      let did = Auth.get_authed_did_exn auth in
      let%lwt {email; token} = Xrpc.parse_body req input_of_yojson in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.invalid_request ~name:"AccountNotFound" "account not found"
      | Some actor -> (
        match%lwt confirm_email ~email ~token actor db with
        | Ok () ->
            Dream.empty `OK
        | Error EmailMismatch ->
            Errors.invalid_request ~name:"InvalidEmail" "email does not match"
        | Error ExpiredToken ->
            Errors.invalid_request ~name:"ExpiredToken" "token expired"
        | Error InvalidToken ->
            Errors.invalid_request ~name:"InvalidToken" "invalid token" ) )
