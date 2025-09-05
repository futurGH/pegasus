open Pegasus

let handlers =
  [ ("/xrpc/_health", Api.Health.handler)
  ; ("/.well-known/did.json", Api.Well_known.did_json)
  ; ( "/xrpc/com.atproto.server.describeServer"
    , Api.Server.DescribeServer.handler )
  ; ("/xrpc/com.atproto.server.createSession", Api.Server.CreateSession.handler)
  ; ( "/xrpc/com.atproto.server.refreshSession"
    , Api.Server.RefreshSession.handler )
  ; ("/xrpc/com.atproto.server.getSession", Api.Server.GetSession.handler)
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
