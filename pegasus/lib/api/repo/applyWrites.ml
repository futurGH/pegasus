type request =
  { repo: string
  ; validate: bool option [@default None]
  ; writes: Repository.repo_write list
  ; swap_commit: string option [@key "swapCommit"] [@default None] }
[@@deriving yojson]

type response =
  {commit: res_commit option; results: Repository.apply_writes_result list}
[@@deriving yojson]

and res_commit = {cid: string; rev: string} [@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Authorization (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req request_of_yojson in
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      let%lwt repo = Repository.load did in
      let%lwt {commit= commit_cid, {rev; _}; results} =
        Repository.apply_writes repo input.writes
          (Option.map Cid.as_cid input.swap_commit)
      in
      Dream.json @@ Yojson.Safe.to_string
      @@ response_to_yojson
           {commit= Some {cid= Cid.to_string commit_cid; rev}; results} )
