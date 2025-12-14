type request =
  { repo: string
  ; validate: bool option [@default None]
  ; writes: Repository.repo_write list
  ; swap_commit: string option [@key "swapCommit"] [@default None] }
[@@deriving yojson {strict= false}]

type response =
  { commit: res_commit option [@default None]
  ; results: Repository.apply_writes_result list }
[@@deriving yojson {strict= false}]

and res_commit = {cid: string; rev: string} [@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Authorization (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req request_of_yojson in
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      (* check oauth scopes for each write operation *)
      List.iter
        (fun (write : Repository.repo_write) ->
          match write with
          | Create {collection; _} ->
              Auth.assert_repo_scope ctx.auth ~collection
                ~action:Oauth.Scopes.Create
          | Update {collection; _} ->
              Auth.assert_repo_scope ctx.auth ~collection
                ~action:Oauth.Scopes.Update
          | Delete {collection; _} ->
              Auth.assert_repo_scope ctx.auth ~collection
                ~action:Oauth.Scopes.Delete )
        input.writes ;
      let%lwt repo = Repository.load did in
      let%lwt {commit= commit_cid, {rev; _}; results} =
        Repository.apply_writes repo input.writes
          (Option.map Cid.as_cid input.swap_commit)
      in
      Dream.json @@ Yojson.Safe.to_string
      @@ response_to_yojson
           {commit= Some {cid= Cid.to_string commit_cid; rev}; results} )
