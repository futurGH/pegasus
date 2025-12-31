let handler =
  Xrpc.handler (fun ctx ->
      let did = Dream.query ctx.req "did" in
      let%lwt () =
        match did with
        | Some did ->
            Session.log_out_did ctx.req did
        | None ->
            Session.log_out_all_dids ctx.req
      in
      Dream.redirect ctx.req "/account" )
