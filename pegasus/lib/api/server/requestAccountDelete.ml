let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some _actor ->
          let code =
            "del-"
            ^ String.sub
                Digestif.SHA256.(
                  digest_string (did ^ Int.to_string @@ Util.now_ms ())
                  |> to_hex )
                0 8
          in
          let expires_at = Util.now_ms () + (15 * 60 * 1000) in
          let%lwt () = Data_store.set_auth_code ~did ~code ~expires_at db in
          Dream.log "account deletion code for %s: %s" did code ;
          Dream.empty `OK )
