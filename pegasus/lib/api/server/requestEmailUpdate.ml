open Lexicons.Com.Atproto.Server.RequestEmailUpdate.Main

let request_email_update ?pending_email (actor : Data_store.Types.actor) db =
  let token_required =
    Option.is_none actor.email_confirmed_at || Option.is_some pending_email
  in
  let%lwt () =
    if token_required then
      let did = actor.did in
      let code = Util.make_code () in
      let expires_at = Util.now_ms () + (10 * 60 * 1000) in
      let%lwt () =
        match pending_email with
        | Some pending_email ->
            Data_store.set_pending_email ~did ~code ~expires_at ~pending_email
              db
        | None ->
            Data_store.set_auth_code ~did ~code ~expires_at db
      in
      let to_email =
        (* if we're trying to change unconfirmed email, send confirmation to new email
           to avoid e.g. getting stuck with an invalid email address *)
        match (actor.email_confirmed_at, pending_email) with
        | None, Some pending_email ->
            pending_email
        | _ ->
            actor.email
      in
      Util.send_email_or_log ~recipients:[To to_email]
        ~subject:(Printf.sprintf "Confirm email change for %s" actor.handle)
        ~body:
          (Emails.EmailUpdate.make ~handle:actor.handle ~new_email:pending_email
             ~code )
    else Lwt.return_unit
  in
  Lwt.return token_required

let calc_key_did ctx = Some (Auth.get_authed_did_exn ctx.Xrpc.auth)

let handler =
  Xrpc.handler ~auth:Authorization
    ~rate_limits:
      [ Route
          { duration_ms= Util.day
          ; points= 15
          ; calc_key= Some calc_key_did
          ; calc_points= None }
      ; Route
          { duration_ms= Util.hour
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
      | Some actor ->
          let%lwt token_required = request_email_update actor db in
          Dream.json @@ Yojson.Safe.to_string
          @@ output_to_yojson {token_required} )
