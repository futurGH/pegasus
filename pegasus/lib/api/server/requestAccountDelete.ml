let request_account_delete (actor : Data_store.Types.actor) db =
  let did = actor.did in
  let code =
    "del-"
    ^ String.sub
        Digestif.SHA256.(
          digest_string (did ^ Int.to_string @@ Util.now_ms ()) |> to_hex )
        0 8
  in
  let expires_at = Util.now_ms () + (15 * 60 * 1000) in
  let%lwt () = Data_store.set_auth_code ~did ~code ~expires_at db in
  Util.send_email_or_log ~recipients:[To actor.email]
    ~subject:(Printf.sprintf "Account deletion request for %s" actor.handle)
    ~body:
      (Plain
         (Printf.sprintf "Delete your account using the following token: %s"
            code ) )

let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor ->
          let%lwt () = request_account_delete actor db in
          Dream.empty `OK )
