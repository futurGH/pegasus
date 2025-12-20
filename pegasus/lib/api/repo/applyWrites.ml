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

let calc_write_points (writes : Repository.repo_write list) =
  List.fold_left
    (fun acc (write : Repository.repo_write) ->
      acc + match write with Create _ -> 3 | Update _ -> 2 | Delete _ -> 1 )
    0 writes

let handler =
  Xrpc.handler ~auth:Authorization (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req request_of_yojson in
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      (* apply rate limits after parsing body so we can count points accurately *)
      let points = calc_write_points input.writes in
      let _ =
        Xrpc.consume_shared_rate_limit ~name:"repo-write-hour" ~key:did ~points
      in
      let _ =
        Xrpc.consume_shared_rate_limit ~name:"repo-write-day" ~key:did ~points
      in
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
