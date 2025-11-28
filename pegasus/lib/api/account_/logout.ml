let handler =
  Xrpc.handler (fun ctx ->
      let%lwt () = Session.Raw.clear_session ctx.req in
      Dream.redirect ctx.req "/account/login" )
