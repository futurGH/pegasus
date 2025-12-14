let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      let code =
        "plc-"
        ^ Uuidm.to_string (Uuidm.v4_gen (Random.State.make_self_init ()) ())
      in
      let expires_at = Util.now_ms () + (60 * 60 * 1000) in
      let%lwt () = Data_store.set_auth_code ~did ~code ~expires_at db in
      (* TODO: something that isn't this *)
      Dream.log "auth code for %s: %s" did code ;
      Dream.empty `OK )
