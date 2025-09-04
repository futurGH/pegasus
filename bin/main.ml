open Pegasus

let handlers =
  [ ( "/xrpc/com.atproto.server.describeServer"
    , Api.Server.DescribeServer.handler )
  ; ("/xrpc/com.atproto.sync.subscribeRepos", Api.Server.SubscribeRepos.handler)
  ]

let main =
  let%lwt db = Util.connect_sqlite Util.Constants.pegasus_db_location in
  let%lwt () = Data_store.init db in
  Dream.serve ~interface:"0.0.0.0" ~port:8008
  @@ Dream.logger @@ Dream.router
  @@ List.map
       (fun (path, handler) ->
         Dream.get path (fun req -> handler ({req; db} : Xrpc.init)) )
       handlers

let () = Lwt_main.run main
