type request =
  { repo: string
  ; collection: string
  ; rkey: string
  ; swap_record: string option [@key "swapRecord"] [@default None]
  ; swap_commit: string option [@key "swapCommit"] [@default None] }
[@@deriving yojson]

type response = {commit: res_commit option} [@@deriving yojson]

and res_commit = {cid: string; rev: string} [@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Auth.Verifiers.authorization (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req request_of_yojson in
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      let%lwt repo = Repository.load did in
      let write : Repository.repo_write =
        Delete
          { type'= Repository.Write_op.delete
          ; collection= input.collection
          ; rkey= input.rkey
          ; swap_record= Option.map Cid.as_cid input.swap_record }
      in
      let%lwt {commit= commit_cid, {rev; _}; results} =
        Repository.apply_writes repo [write]
          (Option.map Cid.as_cid input.swap_commit)
      in
      match List.hd results with
      | Delete _ ->
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson
               {commit= Some {cid= Cid.to_string commit_cid; rev}}
      | _ ->
          Errors.invalid_request "unexpected create or update result" )
