open Lexicons.Com.Atproto.Server.RequestPasswordReset.Main

let request_password_reset (actor : Data_store.Types.actor) db =
  let did = actor.did in
  let code = Util.make_code () in
  let expires_at = Util.Time.now_ms () + (10 * 60 * 1000) in
  let%lwt () = Data_store.set_auth_code ~did ~code ~expires_at db in
  Util.send_email_or_log ~recipients:[To actor.email]
    ~subject:(Printf.sprintf "Password reset for %s" actor.handle)
    ~body:(Emails.PasswordReset.make ~handle:actor.handle ~code)

let handler =
  Xrpc.handler
    ~rate_limits:
      [ Route
          { duration_ms= Util.Time.day
          ; points= 50
          ; calc_key= None
          ; calc_points= None }
      ; Route
          { duration_ms= Util.Time.hour
          ; points= 15
          ; calc_key= None
          ; calc_points= None } ]
    (fun {req; auth; db; _} ->
      let%lwt actor_opt =
        match auth with
        | Auth.Access {did} | Auth.OAuth {did; _} -> (
          match%lwt Data_store.get_actor_by_identifier did db with
          | Some actor ->
              Lwt.return_some actor
          | None ->
              Errors.internal_error ~msg:"actor not found" () )
        | _ -> (
            let%lwt {email} = Xrpc.parse_body req input_of_yojson in
            let email = String.lowercase_ascii email in
            match%lwt Data_store.get_actor_by_identifier email db with
            | Some actor ->
                Lwt.return_some actor
            | None ->
                Lwt.return_none )
      in
      match actor_opt with
      | None ->
          (* always return success to prevent email enumeration *)
          Dream.empty `OK
      | Some actor ->
          let%lwt () = request_password_reset actor db in
          Dream.empty `OK )
