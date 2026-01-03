open Lexicons.Com.Atproto.Server.CheckAccountStatus.Main

let get_account_status did =
  let%lwt {db= us; commit; actor; _} = Repository.load did in
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
      Lwt.return_ok
        { activated= actor.deactivated_at = None
        ; valid_did= true
        ; repo_commit
        ; repo_rev
        ; repo_blocks
        ; indexed_records
        ; private_state_values= 0
        ; expected_blobs
        ; imported_blobs }
  | _ ->
      Lwt.return_error "failed to load account data"

let handler =
  Xrpc.handler (fun {auth; _} ->
      let did = Auth.get_authed_did_exn auth in
      match%lwt get_account_status did with
      | Ok status ->
          status |> output_to_yojson |> Yojson.Safe.to_string |> Dream.json
      | Error msg ->
          Errors.internal_error ~msg () )
