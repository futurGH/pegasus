type response =
  { activated: bool
  ; valid_did: bool [@key "validDid"]
  ; repo_commit: string [@key "repoCommit"]
  ; repo_rev: string [@key "repoRev"]
  ; repo_blocks: int [@key "repoBlocks"]
  ; indexed_records: int [@key "indexedRecords"]
  ; private_state_values: int [@key "privateStateValues"]
  ; expected_blobs: int [@key "expectedBlobs"]
  ; imported_blobs: int [@key "importedBlobs"] }
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun {db; auth; _} ->
      let did = Auth.get_authed_did_exn auth in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor -> (
          let%lwt {db= us; commit; _} =
            Repository.load ~ds:db did
          in
          let%lwt cid, commit =
            match commit with
            | Some c ->
                Lwt.return c
            | None ->
                User_store.get_commit us |> Lwt.map Option.get
          in
          let repo_commit, repo_rev = (Cid.to_string cid, commit.rev) in
          match%lwt
            Lwt.all
              [ User_store.count_blocks us
              ; User_store.count_records us
              ; User_store.count_blobs us
              ; User_store.count_referenced_blobs us ]
          with
          | [block_count; indexed_records; imported_blobs; expected_blobs] ->
              (* mst blocks + records + commit *)
              let repo_blocks = block_count + indexed_records + 1 in
              { activated= actor.deactivated_at <> None
              ; valid_did= true
              ; repo_commit
              ; repo_rev
              ; repo_blocks
              ; indexed_records
              ; private_state_values= 0
              ; expected_blobs
              ; imported_blobs }
              |> response_to_yojson |> Yojson.Safe.to_string |> Dream.json
          | _ ->
              Errors.internal_error ~msg:"failed to load account data" () ) )
