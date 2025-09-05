let handler =
  Xrpc.handler (fun _ ->
      Dream.respond {|
# crawl away 🚀
User-Agent: *
Allow: /
|} )
