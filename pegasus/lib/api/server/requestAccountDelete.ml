let request_account_delete (actor : Data_store.Types.actor) db =
  let did = actor.did in
  let code =
    "del-"
    ^ String.sub
        Digestif.SHA256.(
          digest_string (did ^ Int.to_string @@ Util.Time.now_ms ()) |> to_hex )
        0 8
  in
  let expires_at = Util.Time.now_ms () + (15 * 60 * 1000) in
  let%lwt () = Data_store.set_auth_code ~did ~code ~expires_at db in
  Util.send_email_or_log ~recipients:[To actor.email]
    ~subject:(Printf.sprintf "Account deletion request for %s" actor.handle)
    ~body:(Emails.AccountDelete.make ~handle:actor.handle ~code)

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
      let did = Auth.get_authed_did_exn auth in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor ->
          let%lwt () = request_account_delete actor db in
          Dream.empty `OK )
