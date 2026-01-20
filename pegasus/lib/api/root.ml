let handler =
  Xrpc.handler (fun _ ->
      Util.Html.render_page ~title:"Pegasus" (module Frontend.RootPage) ~props:() )
