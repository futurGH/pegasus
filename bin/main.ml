open Pegasus
open Dream

let handlers =
  [ (* meta *)
    (get, "/", Api.Root.handler)
  ; (get, "/robots.txt", Api.Robots.handler)
  ; (get, "/xrpc/_health", Api.Health.handler)
  ; (get, "/.well-known/did.json", Api.Well_known.did_json)
  ; (* unauthed *)
    ( get
    , "/xrpc/com.atproto.server.describeServer"
    , Api.Server.DescribeServer.handler )
  ; ( get
    , "/xrpc/com.atproto.identity.resolveHandle"
    , Api.Identity.ResolveHandle.handler )
  ; ( get
    , "/xrpc/com.atproto.sync.subscribeRepos"
    , Api.Server.SubscribeRepos.handler )
  ; ( post
    , "/xrpc/com.atproto.server.createSession"
    , Api.Server.CreateSession.handler )
  ; (get, "/xrpc/com.atproto.repo.getRecord", Api.Repo.GetRecord.handler)
  ; (get, "/xrpc/com.atproto.repo.listRecords", Api.Repo.ListRecords.handler)
  ; (* account *)
    (get, "/xrpc/com.atproto.server.getSession", Api.Server.GetSession.handler)
  ; ( post
    , "/xrpc/com.atproto.server.refreshSession"
    , Api.Server.RefreshSession.handler )
  ; ( post
    , "/xrpc/com.atproto.server.deleteSession"
    , Api.Server.DeleteSession.handler )
  ; ( post
    , "/xrpc/com.atproto.identity.updateHandle"
    , Api.Identity.UpdateHandle.handler )
  ; (* repo *)
    (post, "/xrpc/com.atproto.repo.applyWrites", Api.Repo.ApplyWrites.handler)
  ; (post, "/xrpc/com.atproto.repo.createRecord", Api.Repo.CreateRecord.handler)
  ; (post, "/xrpc/com.atproto.repo.putRecord", Api.Repo.PutRecord.handler)
  ; (post, "/xrpc/com.atproto.repo.deleteRecord", Api.Repo.DeleteRecord.handler)
  ; (post, "/xrpc/com.atproto.repo.uploadBlob", Api.Repo.UploadBlob.handler)
  ; (* sync *)
    (get, "/xrpc/com.atproto.sync.getRepoStatus", Api.Sync.GetRepoStatus.handler)
  ; (get, "/xrpc/com.atproto.sync.listRepos", Api.Sync.ListRepos.handler)
  ; (get, "/xrpc/com.atproto.sync.getBlob", Api.Sync.GetBlob.handler)
  ; (get, "/xrpc/com.atproto.sync.listBlobs", Api.Sync.ListBlobs.handler)
  ; (* preferences *)
    ( get
    , "/xrpc/com.atproto.actor.getPreferences"
    , Api.Actor.GetPreferences.handler )
  ; ( post
    , "/xrpc/com.atproto.actor.putPreferences"
    , Api.Actor.PutPreferences.handler ) ]

let main =
  let%lwt db = Data_store.connect () in
  let%lwt () = Data_store.init db in
  Dream.serve ~interface:"0.0.0.0" ~port:8008
  @@ Dream.logger
  @@ Xrpc.service_proxy_middleware db
  @@ Dream.router
  @@ List.map
       (fun (fn, path, handler) ->
         fn path (fun req -> handler ({req; db} : Xrpc.init)) )
       handlers

let () = Lwt_main.run main
