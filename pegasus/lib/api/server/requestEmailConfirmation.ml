type request_error = AlreadyConfirmed

let request_email_confirmation (actor : Data_store.Types.actor) db =
  match actor.email_confirmed_at with
  | Some _ ->
      Lwt.return_error AlreadyConfirmed
  | None ->
      let code = Util.make_code () in
      let expires_at = Util.Time.now_ms () + (10 * 60 * 1000) in
      let%lwt () =
        Data_store.set_auth_code ~did:actor.did ~code ~expires_at db
      in
      let%lwt () =
        Util.send_email_or_log ~recipients:[To actor.email]
          ~subject:(Printf.sprintf "Confirm email for %s" actor.handle)
          ~body:(Emails.EmailConfirmation.make ~handle:actor.handle ~code)
      in
      Lwt.return_ok ()

let calc_key_did ctx = Some (Auth.get_authed_did_exn ctx.Xrpc.auth)

let handler =
  Xrpc.handler ~auth:Authorization
    ~rate_limits:
      [ Route
          { duration_ms= Util.Time.day
          ; points= 15
          ; calc_key= Some calc_key_did
          ; calc_points= None }
      ; Route
          { duration_ms= Util.Time.hour
          ; points= 5
          ; calc_key= Some calc_key_did
          ; calc_points= None } ]
    (fun {auth; db; _} ->
      Auth.assert_account_scope auth ~attr:Oauth.Scopes.Email
        ~action:Oauth.Scopes.Manage ;
      let did = Auth.get_authed_did_exn auth in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor -> (
        match%lwt request_email_confirmation actor db with
        | Error AlreadyConfirmed ->
            Errors.invalid_request ~name:"InvalidRequest"
              "email already confirmed"
        | _ ->
            Dream.empty `OK ) )
