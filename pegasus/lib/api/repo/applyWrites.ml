type request =
  { repo: string
  ; validate: bool option
  ; writes: Repository.repo_write list
  ; swap_commit: Cid.t option [@key "swapCommit"] }
[@@deriving yojson]

type response =
  {commit: res_commit option; results: Repository.apply_writes_result list}
[@@deriving yojson]

and res_commit = {cid: Cid.t; rev: string} [@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Auth.Verifiers.authorization (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req request_of_yojson in
      let%lwt input_did =
        if String.starts_with ~prefix:"did:" input.repo then
          Lwt.return input.repo
        else
          match%lwt Data_store.get_actor_by_identifier input.repo ctx.db with
          | Some {did; _} ->
              Lwt.return did
          | None ->
              Errors.invalid_request "target repository not found"
      in
      let did =
        match ctx.auth with
        | Access {did} when did = input_did ->
            did
        | Admin ->
            input_did
        | _ ->
            Errors.auth_required
              "authentication does not match target repository"
      in
      let%lwt repo = Repository.load did in
      let%lwt {commit= commit_cid, {rev; _}; results} =
        Repository.apply_writes repo input.writes input.swap_commit
      in
      Dream.json @@ Yojson.Safe.to_string
      @@ response_to_yojson {commit= Some {cid= commit_cid; rev}; results} )
