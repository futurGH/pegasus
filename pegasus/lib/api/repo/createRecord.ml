type request =
  { repo: string
  ; collection: string
  ; rkey: string option [@default None]
  ; validate: bool option [@default None]
  ; record: Mist.Lex.repo_record
  ; swap_record: string option [@key "swapRecord"] [@default None]
  ; swap_commit: string option [@key "swapCommit"] [@default None] }
[@@deriving yojson]

type response =
  { uri: string
  ; cid: string
  ; commit: res_commit option
  ; validation_status: string option [@key "validationStatus"] }
[@@deriving yojson]

and res_commit = {cid: string; rev: string} [@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Authorization (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req request_of_yojson in
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      let%lwt repo = Repository.load did in
      let write : Repository.repo_write =
        match input.swap_record with
        | Some swap_record -> (
          match input.rkey with
          | Some rkey ->
              Update
                { type'= Repository.Write_op.update
                ; collection= input.collection
                ; rkey
                ; value= input.record
                ; swap_record= Some (Cid.as_cid swap_record) }
          | None ->
              Errors.invalid_request "rkey is required for swap_record" )
        | None ->
            Create
              { type'= Repository.Write_op.create
              ; collection= input.collection
              ; rkey= input.rkey
              ; value= input.record }
      in
      let%lwt {commit= commit_cid, {rev; _}; results} =
        Repository.apply_writes repo [write]
          (Option.map Cid.as_cid input.swap_commit)
      in
      match List.hd results with
      | Create {uri; cid; _} | Update {uri; cid; _} ->
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson
               { uri
               ; cid= Cid.to_string cid
               ; commit= Some {cid= Cid.to_string commit_cid; rev}
               ; validation_status= Some "valid" }
      | _ ->
          Errors.invalid_request "unexpected delete result" )
