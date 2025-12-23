let handler =
  Xrpc.handler (fun _ ->
      Util.render_html ~title:"Pegasus" (module Frontend.RootPage) ~props:() )
