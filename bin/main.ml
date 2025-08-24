let () =
  Dream.run ~interface:"0.0.0.0" ~port:8008
  @@ Dream.logger
  @@ Dream.router
       [ Dream.get "/xrpc/com.atproto.server.describeServer"
           Pegasus.Api.Server.DescribeServer.handler ]
