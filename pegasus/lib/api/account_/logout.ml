let handler =
  Xrpc.handler (fun ctx ->
      let%lwt () = Dream.invalidate_session ctx.req in
      Dream.redirect ctx.req "/account/login" )
