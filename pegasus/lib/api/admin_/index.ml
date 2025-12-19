let handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | true ->
          Dream.redirect ctx.req "/admin/users"
      | false ->
          Dream.redirect ctx.req "/admin/login" )
