let handler =
  Xrpc.handler ~auth:Bearer (fun ctx ->
      let did = Auth.get_authed_did_exn ctx.auth in
      let%lwt _ = Repository.load ~ensure_active:false did in
      let%lwt () = Data_store.activate_actor did ctx.db in
      let%lwt _ = Sequencer.sequence_account ctx.db ~did ~active:true () in
      Dream.empty `OK )
