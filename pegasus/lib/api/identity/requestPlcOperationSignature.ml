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
      let%lwt {email; handle; _} =
        Data_store.get_actor_by_identifier did db |> Lwt.map Option.get
      in
      let%lwt () =
        Util.send_email_or_log ~recipients:[To email]
          ~subject:"Confirm PLC operation"
          ~body:
            (Plain
               (Printf.sprintf
                  "Confirm that you would like to update your PLC identity for \
                   %s (%s) using the following token: %s"
                  handle did code ) )
      in
      Dream.empty `OK )
