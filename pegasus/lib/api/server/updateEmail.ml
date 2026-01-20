open Lexicons.Com.Atproto.Server.UpdateEmail.Main

type update_email_error =
  | TokenRequired
  | ExpiredToken
  | InvalidToken
  | NoEmailProvided

let update_email ?email ~token (actor : Data_store.Types.actor) db =
  let did = actor.did in
  (* use provided email, or fall back to pending_email *)
  let target_email =
    match email with Some e -> Some e | None -> actor.pending_email
  in
  match target_email with
  | None ->
      Lwt.return_error NoEmailProvided
  | Some email -> (
    match actor.email_confirmed_at with
    | Some _ -> (
      (* email is confirmed, require valid token *)
      match token with
      | None ->
          Lwt.return_error TokenRequired
      | Some token -> (
        match (actor.auth_code, actor.auth_code_expires_at) with
        | Some auth_code, Some expires_at
          when auth_code = token && Util.Time.now_ms () < expires_at ->
            let%lwt () = Data_store.update_email ~did ~email db in
            Lwt.return_ok email
        | Some _, Some expires_at when Util.Time.now_ms () >= expires_at ->
            Lwt.return_error ExpiredToken
        | _ ->
            Lwt.return_error InvalidToken ) )
    | None ->
        (* email not confirmed, no token required *)
        let%lwt () = Data_store.update_email ~did ~email db in
        Lwt.return_ok email )

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      Auth.assert_account_scope auth ~attr:Oauth.Scopes.Email
        ~action:Oauth.Scopes.Manage ;
      let did = Auth.get_authed_did_exn auth in
      let%lwt {email; token; _} = Xrpc.parse_body req input_of_yojson in
      let email = String.lowercase_ascii email in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor -> (
        match%lwt update_email ~email ~token actor db with
        | Ok _ ->
            Dream.empty `OK
        | Error TokenRequired ->
            Errors.invalid_request ~name:"TokenRequired"
              "confirmation token required"
        | Error ExpiredToken ->
            Errors.invalid_request ~name:"ExpiredToken" "token expired"
        | Error InvalidToken ->
            Errors.invalid_request ~name:"InvalidToken" "invalid token"
        | Error NoEmailProvided ->
            Errors.invalid_request ~name:"InvalidRequest" "email is required" ) )
