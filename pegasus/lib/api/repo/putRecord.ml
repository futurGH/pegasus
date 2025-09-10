type request =
  { repo: string
  ; collection: string
  ; rkey: string
  ; validate: bool option
  ; record: Mist.Lex.repo_record
  ; swap_record: Cid.t option [@key "swapRecord"]
  ; swap_commit: Cid.t option [@key "swapCommit"] }
[@@deriving yojson]

type response =
  { uri: string
  ; cid: Cid.t
  ; commit: res_commit option
  ; validation_status: string option [@key "validationStatus"] }
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
      let write : Repository.repo_write =
        Update
          { type'= Repository.Write_op.update
          ; collection= input.collection
          ; rkey= input.rkey
          ; value= input.record
          ; swap_record= input.swap_record }
      in
      let%lwt {commit= commit_cid, {rev; _}; results} =
        Repository.apply_writes repo [write] input.swap_commit
      in
      match List.hd results with
      | Create {uri; cid; _} | Update {uri; cid; _} ->
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson
               { uri
               ; cid
               ; commit= Some {cid= commit_cid; rev}
               ; validation_status= Some "valid" }
      | _ ->
          Errors.invalid_request "unexpected delete result" )
