open Pegasus
open Dream

let () =
  Rate_limiter.Shared.register ~name:"repo-write-hour" ~duration_ms:Util.hour
    ~points:5000 ;
  Rate_limiter.Shared.register ~name:"repo-write-day" ~duration_ms:Util.day
    ~points:35000

let handlers =
  [ (* meta *)
    (get, "/", Api.Root.handler)
  ; (get, "/robots.txt", Api.Robots.handler)
  ; (get, "/xrpc/_health", Api.Health.handler)
  ; (get, "/.well-known/did.json", Api.Well_known.did_json)
  ; ( get
    , "/.well-known/oauth-protected-resource"
    , Api.Well_known.oauth_protected_resource )
  ; ( get
    , "/.well-known/oauth-authorization-server"
    , Api.Well_known.oauth_authorization_server )
  ; (get, "/.well-known/atproto-did", Api.Well_known.atproto_did)
  ; (options, "/xrpc/**", Xrpc.handler (fun _ -> Dream.empty `No_Content))
  ; (* oauth *)
    (options, "/oauth/par", Xrpc.handler (fun _ -> Dream.empty `No_Content))
  ; (post, "/oauth/par", Api.Oauth_.Par.post_handler)
  ; (get, "/oauth/authorize", Api.Oauth_.Authorize.get_handler)
  ; (post, "/oauth/authorize", Api.Oauth_.Authorize.post_handler)
  ; (options, "/oauth/token", Xrpc.handler (fun _ -> Dream.empty `No_Content))
  ; (post, "/oauth/token", Api.Oauth_.Token.post_handler)
  ; (* account ui *)
    (get, "/account", Api.Account_.Index.get_handler)
  ; (post, "/account", Api.Account_.Index.post_handler)
  ; (get, "/account/permissions", Api.Account_.Permissions.get_handler)
  ; (post, "/account/permissions", Api.Account_.Permissions.post_handler)
  ; (get, "/account/login", Api.Account_.Login.get_handler)
  ; (post, "/account/login", Api.Account_.Login.post_handler)
  ; (get, "/account/signup", Api.Account_.Signup.get_handler)
  ; (post, "/account/signup", Api.Account_.Signup.post_handler)
  ; ( get
    , "/account/signup/check-handle"
    , Api.Account_.Signup.check_handle_handler )
  ; (get, "/account/logout", Api.Account_.Logout.handler)
  ; (* admin ui *)
    (get, "/admin", Api.Admin_.Index.handler)
  ; (get, "/admin/login", Api.Admin_.Login.get_handler)
  ; (post, "/admin/login", Api.Admin_.Login.post_handler)
  ; (get, "/admin/users", Api.Admin_.Users.get_handler)
  ; (post, "/admin/users", Api.Admin_.Users.post_handler)
  ; (get, "/admin/invites", Api.Admin_.Invites.get_handler)
  ; (post, "/admin/invites", Api.Admin_.Invites.post_handler)
  ; (* unauthed *)
    ( get
    , "/xrpc/com.atproto.server.describeServer"
    , Api.Server.DescribeServer.handler )
  ; (get, "/xrpc/com.atproto.repo.describeRepo", Api.Repo.DescribeRepo.handler)
  ; ( get
    , "/xrpc/com.atproto.identity.resolveHandle"
    , Api.Identity.ResolveHandle.handler )
  ; (* admin *)
    ( post
    , "/xrpc/com.atproto.admin.deleteAccount"
    , Api.Admin.DeleteAccount.handler )
  ; ( get
    , "/xrpc/com.atproto.admin.getAccountInfo"
    , Api.Admin.GetAccountInfo.handler )
  ; ( get
    , "/xrpc/com.atproto.admin.getAccountInfos"
    , Api.Admin.GetAccountInfos.handler )
  ; ( get
    , "/xrpc/com.atproto.admin.getInviteCodes"
    , Api.Admin.GetInviteCodes.handler )
  ; (post, "/xrpc/com.atproto.admin.sendEmail", Api.Admin.SendEmail.handler)
  ; ( post
    , "/xrpc/com.atproto.admin.updateAccountEmail"
    , Api.Admin.UpdateAccountEmail.handler )
  ; ( post
    , "/xrpc/com.atproto.admin.updateAccountHandle"
    , Api.Admin.UpdateAccountHandle.handler )
  ; (* account management *)
    ( post
    , "/xrpc/com.atproto.server.createInviteCode"
    , Api.Server.CreateInviteCode.handler )
  ; ( post
    , "/xrpc/com.atproto.server.createInviteCodes"
    , Api.Server.CreateInviteCodes.handler )
  ; ( post
    , "/xrpc/com.atproto.server.createAccount"
    , Api.Server.CreateAccount.handler )
  ; ( post
    , "/xrpc/com.atproto.server.createSession"
    , Api.Server.CreateSession.handler )
  ; (get, "/xrpc/com.atproto.server.getSession", Api.Server.GetSession.handler)
  ; ( post
    , "/xrpc/com.atproto.server.refreshSession"
    , Api.Server.RefreshSession.handler )
  ; ( post
    , "/xrpc/com.atproto.server.deleteSession"
    , Api.Server.DeleteSession.handler )
  ; ( get
    , "/xrpc/com.atproto.server.checkAccountStatus"
    , Api.Server.CheckAccountStatus.handler )
  ; ( post
    , "/xrpc/com.atproto.server.activateAccount"
    , Api.Server.ActivateAccount.handler )
  ; ( post
    , "/xrpc/com.atproto.server.requestEmailConfirmation"
    , Api.Server.RequestEmailConfirmation.handler )
  ; ( post
    , "/xrpc/com.atproto.server.requestEmailUpdate"
    , Api.Server.RequestEmailUpdate.handler )
  ; ( post
    , "/xrpc/com.atproto.server.confirmEmail"
    , Api.Server.ConfirmEmail.handler )
  ; ( post
    , "/xrpc/com.atproto.server.requestPasswordReset"
    , Api.Server.RequestPasswordReset.handler )
  ; ( post
    , "/xrpc/com.atproto.server.resetPassword"
    , Api.Server.ResetPassword.handler )
  ; ( post
    , "/xrpc/com.atproto.server.reserveSigningKey"
    , Api.Server.ReserveSigningKey.handler )
  ; ( post
    , "/xrpc/com.atproto.server.requestAccountDelete"
    , Api.Server.RequestAccountDelete.handler )
  ; ( post
    , "/xrpc/com.atproto.server.deleteAccount"
    , Api.Server.DeleteAccount.handler )
  ; ( post
    , "/xrpc/com.atproto.server.deactivateAccount"
    , Api.Server.DeactivateAccount.handler )
  ; ( get
    , "/xrpc/com.atproto.repo.listMissingBlobs"
    , Api.Repo.ListMissingBlobs.handler )
  ; ( post
    , "/xrpc/com.atproto.identity.updateHandle"
    , Api.Identity.UpdateHandle.handler )
  ; ( post
    , "/xrpc/com.atproto.server.updateEmail"
    , Api.Server.UpdateEmail.handler )
  ; (* plc *)
    ( get
    , "/xrpc/com.atproto.identity.getRecommendedDidCredentials"
    , Api.Identity.GetRecommendedDidCredentials.handler )
  ; ( post
    , "/xrpc/com.atproto.identity.requestPlcOperationSignature"
    , Api.Identity.RequestPlcOperationSignature.handler )
  ; ( post
    , "/xrpc/com.atproto.identity.signPlcOperation"
    , Api.Identity.SignPlcOperation.handler )
  ; ( post
    , "/xrpc/com.atproto.identity.submitPlcOperation"
    , Api.Identity.SubmitPlcOperation.handler )
  ; (* repo *)
    (post, "/xrpc/com.atproto.repo.applyWrites", Api.Repo.ApplyWrites.handler)
  ; (post, "/xrpc/com.atproto.repo.createRecord", Api.Repo.CreateRecord.handler)
  ; (post, "/xrpc/com.atproto.repo.putRecord", Api.Repo.PutRecord.handler)
  ; (get, "/xrpc/com.atproto.repo.getRecord", Api.Repo.GetRecord.handler)
  ; (get, "/xrpc/com.atproto.repo.listRecords", Api.Repo.ListRecords.handler)
  ; (post, "/xrpc/com.atproto.repo.deleteRecord", Api.Repo.DeleteRecord.handler)
  ; (post, "/xrpc/com.atproto.repo.uploadBlob", Api.Repo.UploadBlob.handler)
  ; (post, "/xrpc/com.atproto.repo.importRepo", Api.Repo.ImportRepo.handler)
  ; (* sync *)
    (get, "/xrpc/com.atproto.sync.getRepo", Api.Sync.GetRepo.handler)
  ; (get, "/xrpc/com.atproto.sync.getRepoStatus", Api.Sync.GetRepoStatus.handler)
  ; ( get
    , "/xrpc/com.atproto.sync.getLatestCommit"
    , Api.Sync.GetLatestCommit.handler )
  ; (get, "/xrpc/com.atproto.sync.listRepos", Api.Sync.ListRepos.handler)
  ; (get, "/xrpc/com.atproto.sync.getRecord", Api.Sync.GetRecord.handler)
  ; (get, "/xrpc/com.atproto.sync.getBlocks", Api.Sync.GetBlocks.handler)
  ; (get, "/xrpc/com.atproto.sync.getBlob", Api.Sync.GetBlob.handler)
  ; (get, "/xrpc/com.atproto.sync.listBlobs", Api.Sync.ListBlobs.handler)
  ; ( get
    , "/xrpc/com.atproto.sync.subscribeRepos"
    , Api.Sync.SubscribeRepos.handler )
  ; (* misc *)
    ( get
    , "/xrpc/app.bsky.actor.getPreferences"
    , Api.Proxy.AppBskyActorGetPreferences.handler )
  ; ( post
    , "/xrpc/app.bsky.actor.putPreferences"
    , Api.Proxy.AppBskyActorPutPreferences.handler )
  ; (get, "/xrpc/app.bsky.feed.getFeed", Api.Proxy.AppBskyFeedGetFeed.handler)
  ]

let public_loader _root path _request =
  match Public.read path with
  | None ->
      Dream.empty `Not_Found
  | Some asset ->
      Dream.respond
        ~headers:[("Cache-Control", "public, max-age=31536000")]
        asset

let static_routes =
  [Dream.get "/public/**" (Dream.static ~loader:public_loader "")]

let main =
  Printexc.record_backtrace true ;
  let%lwt db = Data_store.connect ~create:true () in
  let%lwt () = Data_store.init db in
  Dream.serve ~interface:"0.0.0.0" ~port:8008
  @@ Dream.pipeline
       [ Dream.logger
       ; Dream.set_secret (Env.jwt_key |> Kleidos.privkey_to_multikey)
       ; Dream.cookie_sessions
       ; Dream.livereload
       ; Xrpc.dpop_middleware
       ; Xrpc.cors_middleware ]
  @@ Dream.router
  @@ List.map
       (fun (fn, path, handler) ->
         fn path (fun req -> handler ({req; db} : Xrpc.init)) )
       handlers
  @ static_routes
  @ [ Dream.get "/xrpc/**" (Xrpc.service_proxy_handler db)
    ; Dream.post "/xrpc/**" (Xrpc.service_proxy_handler db) ]

let () = Lwt_main.run main
