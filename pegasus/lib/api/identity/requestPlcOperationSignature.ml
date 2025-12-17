let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      let code =
        "plc-"
        ^ String.sub
            Digestif.SHA256.(
              digest_string (did ^ Int.to_string @@ Util.now_ms ()) |> to_hex )
            0 8
      in
      let expires_at = Util.now_ms () + (60 * 60 * 1000) in
      let%lwt () = Data_store.set_auth_code ~did ~code ~expires_at db in
      (* TODO: something that isn't this *)
      Dream.log "auth code for %s: %s" did code ;
      Dream.empty `OK )
