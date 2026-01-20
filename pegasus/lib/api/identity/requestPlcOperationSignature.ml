let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      let code = Util.make_code () in
      let expires_at = Util.Time.now_ms () + (60 * 60 * 1000) in
      let%lwt () = Data_store.set_auth_code ~did ~code ~expires_at db in
      let%lwt {email; handle; _} =
        Data_store.get_actor_by_identifier did db |> Lwt.map Option.get
      in
      let%lwt () =
        Util.send_email_or_log ~recipients:[To email]
          ~subject:"Confirm PLC operation"
          ~body:(Emails.PlcOperation.make ~handle ~did ~code)
      in
      Dream.empty `OK )
