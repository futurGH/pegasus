open User_store.Types
module Block_map = User_store.Block_map
module Lex = Mist.Lex
module Mst = Mist.Mst.Make (User_store)
module Cached_store = Mist.Storage.Cache_blockstore (User_store)
module Cached_mst = Mist.Mst.Make (Cached_store)
module Mem_mst = Mist.Mst.Make (Mist.Storage.Memory_blockstore)
module String_map = Lex.String_map
module Tid = Mist.Tid

module Write_op = struct
  let create = "com.atproto.repo.applyWrites#create"

  let update = "com.atproto.repo.applyWrites#update"

  let delete = "com.atproto.repo.applyWrites#delete"
end

type repo_write =
  | Create of
      { type': string [@key "$type"] [@default Write_op.create]
      ; collection: string
      ; rkey: string option
      ; value: Lex.repo_record }
  | Update of
      { type': string [@key "$type"] [@default Write_op.update]
      ; collection: string
      ; rkey: string
      ; value: Lex.repo_record
      ; swap_record: Cid.t option [@key "swapRecord"] }
  | Delete of
      { type': string [@key "$type"] [@default Write_op.delete]
      ; collection: string
      ; rkey: string
      ; swap_record: Cid.t option [@key "swapRecord"] }
[@@deriving yojson {strict= false}]

let repo_write_of_yojson (json : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  let type' = member "$type" json |> to_string in
  let collection = member "collection" json |> to_string in
  let rkey = match member "rkey" json with `String s -> Some s | _ -> None in
  try
    let swap_record =
      match member "swapRecord" json with
      | `String s ->
          s |> Cid.of_string |> Result.get_ok |> Option.some
      | _ ->
          None
    in
    match type' with
    | "com.atproto.repo.applyWrites#create" ->
        let value =
          member "value" json |> Lex.repo_record_of_yojson |> Result.get_ok
        in
        Ok (Create {type'; collection; rkey; value})
    | "com.atproto.repo.applyWrites#update" ->
        let value =
          member "value" json |> Lex.repo_record_of_yojson |> Result.get_ok
        in
        Ok
          (Update {type'; collection; rkey= Option.get rkey; value; swap_record})
    | "com.atproto.repo.applyWrites#delete" ->
        Ok (Delete {type'; collection; rkey= Option.get rkey; swap_record})
    | _ ->
        Error "invalid applyWrites write $type"
  with Invalid_argument e -> Error ("invalid property " ^ e)

let repo_write_to_yojson = function
  | Create {type'; collection; rkey; value} ->
      `Assoc
        [ ("$type", `String type')
        ; ("collection", `String collection)
        ; ("rkey", match rkey with Some rkey -> `String rkey | None -> `Null)
        ; ("value", Lex.repo_record_to_yojson value) ]
  | Update {type'; collection; rkey; value; swap_record} ->
      `Assoc
        [ ("$type", `String type')
        ; ("collection", `String collection)
        ; ("rkey", `String rkey)
        ; ("value", Lex.repo_record_to_yojson value)
        ; ( "swapRecord"
          , match swap_record with
            | Some cid ->
                `String (Cid.to_string cid)
            | None ->
                `Null ) ]
  | Delete {type'; collection; rkey; swap_record} ->
      `Assoc
        [ ("$type", `String type')
        ; ("collection", `String collection)
        ; ("rkey", `String rkey)
        ; ( "swapRecord"
          , match swap_record with
            | Some cid ->
                `String (Cid.to_string cid)
            | None ->
                `Null ) ]

type apply_writes_result =
  | Create of {type': string [@key "$type"]; uri: string; cid: Cid.t}
  | Update of {type': string [@key "$type"]; uri: string; cid: Cid.t}
  | Delete of {type': string [@key "$type"]}
[@@deriving yojson]

let apply_writes_result_of_yojson (json : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  let type' = member "$type" json |> to_string in
  try
    match type' with
    | "com.atproto.repo.applyWrites#createResult" ->
        let uri = member "uri" json |> to_string in
        let cid =
          member "cid" json |> to_string |> Cid.of_string |> Result.get_ok
        in
        Ok (Create {type'; uri; cid})
    | "com.atproto.repo.applyWrites#updateResult" ->
        let uri = member "uri" json |> to_string in
        let cid =
          member "cid" json |> to_string |> Cid.of_string |> Result.get_ok
        in
        Ok (Update {type'; uri; cid})
    | "com.atproto.repo.applyWrites#deleteResult" ->
        Ok (Delete {type'})
    | _ ->
        Error "invalid applyWrites result $type"
  with Invalid_argument e -> Error ("invalid property " ^ e)

let apply_writes_result_to_yojson = function
  | Create {type'; uri; cid} ->
      `Assoc
        [ ("$type", `String type')
        ; ("uri", `String uri)
        ; ("cid", `String (Cid.to_string cid)) ]
  | Update {type'; uri; cid} ->
      `Assoc
        [ ("$type", `String type')
        ; ("uri", `String uri)
        ; ("cid", `String (Cid.to_string cid)) ]
  | Delete {type'} ->
      `Assoc [("$type", `String type')]

type write_result =
  {commit: Cid.t * signed_commit; results: apply_writes_result list}

type t =
  { key: Kleidos.key
  ; did: string
  ; actor: Data_store.Types.actor
  ; db: User_store.t
  ; mutable commit: (Cid.t * signed_commit) option }

let get_record_cid t path : Cid.t option Lwt.t =
  User_store.get_record_cid t.db path

let get_record t path : record option Lwt.t = User_store.get_record t.db path

let list_collections t : string list Lwt.t = User_store.list_collections t.db

let list_records t ?limit ?cursor ?reverse collection =
  User_store.list_records t.db ?limit ?cursor ?reverse collection

let sign_commit t commit : signed_commit =
  let msg = commit |> commit_to_yojson |> Dag_cbor.encode_yojson in
  let signature = Kleidos.sign ~privkey:t.key ~msg in
  { did= commit.did
  ; version= commit.version
  ; data= commit.data
  ; rev= commit.rev
  ; prev= commit.prev
  ; signature }

let put_commit t ?(previous : signed_commit option = None) mst_root :
    (Cid.t * signed_commit) Lwt.t =
  let tid_now = Tid.now () in
  let rev =
    match previous with
    | Some c when c.rev >= tid_now -> (
      try
        let ts, clockid = Tid.to_timestamp_us c.rev in
        Tid.of_timestamp_us ~clockid (Int64.succ ts)
      with _ ->
        failwith
          (Format.sprintf
             "unable to produce commit rev greater than current %s; now is %s"
             c.rev tid_now ) )
    | _ ->
        tid_now
  in
  let commit = {version= 3; did= t.did; prev= None; rev; data= mst_root} in
  let signed = sign_commit t commit in
  let%lwt commit_cid =
    User_store.put_commit t.db signed |> Lwt_result.get_exn
  in
  t.commit <- Some (commit_cid, signed) ;
  Lwt.return (commit_cid, signed)

let put_initial_commit t : (Cid.t * signed_commit) Lwt.t =
  let%lwt commit = User_store.get_commit t.db in
  if commit <> None then failwith ("commit already exists for " ^ t.did) ;
  let%lwt {root; _} = Mst.create_empty t.db |> Lwt_result.get_exn in
  put_commit t root

let apply_writes (t : t) (writes : repo_write list) (swap_commit : Cid.t option)
    : write_result Lwt.t =
  let open Sequencer.Types in
  let%lwt prev_commit =
    match%lwt User_store.get_commit t.db with
    | Some (_, commit) ->
        Lwt.return commit
    | None ->
        failwith ("failed to retrieve commit for " ^ t.did)
  in
  if swap_commit <> None && swap_commit <> Option.map fst t.commit then
    Errors.invalid_request ~name:"InvalidSwap"
      (Format.sprintf "swapCommit cid %s did not match last commit cid %s"
         (Cid.to_string (Option.get swap_commit))
         (match t.commit with Some (c, _) -> Cid.to_string c | None -> "null") ) ;
  let cached_store = Cached_store.create t.db in
  let mst : Cached_mst.t ref =
    ref (Cached_mst.create cached_store prev_commit.data)
  in
  (* ops to emit, built in loop because prev_data (previous cid) is otherwise inaccessible *)
  let commit_ops : commit_evt_op list ref = ref [] in
  let added_leaves = ref Block_map.empty in
  let%lwt results =
    Lwt_list.map_s
      (fun (w : repo_write) ->
        match w with
        | Create {collection; rkey; value; _} ->
            let rkey = Option.value rkey ~default:(Tid.now ()) in
            let path = Format.sprintf "%s/%s" collection rkey in
            let uri = Format.sprintf "at://%s/%s" t.did path in
            let%lwt () =
              match%lwt User_store.get_record_cid t.db path with
              | Some cid ->
                  Errors.invalid_request ~name:"InvalidSwap"
                    (Format.sprintf
                       "attempted to write record %s that already exists with \
                        cid %s"
                       path (Cid.to_string cid) )
              | None ->
                  Lwt.return ()
            in
            let record_with_type : Lex.repo_record =
              if String_map.mem "$type" value then value
              else String_map.add "$type" (`String collection) value
            in
            let%lwt cid, block =
              User_store.put_record t.db (`LexMap record_with_type) path
            in
            added_leaves := Block_map.set cid block !added_leaves ;
            commit_ops :=
              !commit_ops @ [{action= `Create; path; cid= Some cid; prev= None}] ;
            let%lwt new_mst = Cached_mst.add !mst path cid in
            mst := new_mst ;
            let refs =
              Util.find_blob_refs value
              |> List.map (fun (r : Mist.Blob_ref.t) -> r.ref)
            in
            let%lwt () =
              match%lwt User_store.put_blob_refs t.db path refs with
              | Ok () ->
                  Lwt.return ()
              | Error err ->
                  raise err
            in
            Lwt.return
              (Create
                 {type'= "com.atproto.repo.applyWrites#createResult"; uri; cid}
              )
        | Update {collection; rkey; value; swap_record; _} ->
            let path = Format.sprintf "%s/%s" collection rkey in
            let uri = Format.sprintf "at://%s/%s" t.did path in
            let%lwt old_cid = User_store.get_record_cid t.db path in
            ( if
                (swap_record <> None && swap_record <> old_cid)
                || (swap_record = None && old_cid = None)
              then
                let cid_str =
                  match old_cid with
                  | Some cid ->
                      Cid.to_string cid
                  | None ->
                      "null"
                in
                Errors.invalid_request ~name:"InvalidSwap"
                  (Format.sprintf "attempted to update record %s with cid %s"
                     path cid_str ) ) ;
            let%lwt () =
              match old_cid with
              | Some _ -> (
                match%lwt User_store.get_record t.db path with
                | Some record ->
                    let refs =
                      Util.find_blob_refs record.value
                      |> List.map (fun (r : Mist.Blob_ref.t) -> r.ref)
                    in
                    if not (List.is_empty refs) then
                      let%lwt _ =
                        User_store.delete_orphaned_blobs_by_record_path t.db
                          path
                      in
                      Lwt.return_unit
                    else Lwt.return_unit
                | None ->
                    Lwt.return_unit )
              | None ->
                  Lwt.return_unit
            in
            let record_with_type : Lex.repo_record =
              if String_map.mem "$type" value then value
              else String_map.add "$type" (`String collection) value
            in
            let%lwt new_cid, new_block =
              User_store.put_record t.db (`LexMap record_with_type) path
            in
            added_leaves := Block_map.set new_cid new_block !added_leaves ;
            commit_ops :=
              !commit_ops
              @ [{action= `Update; path; cid= Some new_cid; prev= old_cid}] ;
            let%lwt new_mst = Cached_mst.add !mst path new_cid in
            mst := new_mst ;
            let refs =
              Util.find_blob_refs value
              |> List.map (fun (r : Mist.Blob_ref.t) -> r.ref)
            in
            let%lwt () =
              match%lwt User_store.put_blob_refs t.db path refs with
              | Ok () ->
                  Lwt.return ()
              | Error err ->
                  raise err
            in
            Lwt.return
              (Update
                 { type'= "com.atproto.repo.applyWrites#updateResult"
                 ; uri
                 ; cid= new_cid } )
        | Delete {collection; rkey; swap_record; _} ->
            let path = Format.sprintf "%s/%s" collection rkey in
            let%lwt cid = User_store.get_record_cid t.db path in
            ( if cid = None || (swap_record <> None && swap_record <> cid) then
                let cid_str =
                  match cid with
                  | Some cid ->
                      Cid.to_string cid
                  | None ->
                      "null"
                in
                Errors.invalid_request ~name:"InvalidSwap"
                  (Format.sprintf "attempted to delete record %s with cid %s"
                     path cid_str ) ) ;
            let%lwt () =
              match%lwt User_store.get_record t.db path with
              | Some record ->
                  let refs =
                    Util.find_blob_refs record.value
                    |> List.map (fun (r : Mist.Blob_ref.t) -> r.ref)
                  in
                  if not (List.is_empty refs) then
                    let%lwt _ =
                      User_store.delete_orphaned_blobs_by_record_path t.db path
                    in
                    Lwt.return_unit
                  else Lwt.return_unit
              | None ->
                  Lwt.return_unit
            in
            let%lwt () = User_store.delete_record t.db path in
            commit_ops :=
              !commit_ops @ [{action= `Delete; path; cid= None; prev= cid}] ;
            let%lwt new_mst = Cached_mst.delete !mst path in
            mst := new_mst ;
            Lwt.return
              (Delete {type'= "com.atproto.repo.applyWrites#deleteResult"}) )
      writes
  in
  let new_mst = !mst in
  (* flush all writes, ensuring all blocks are written or none are *)
  let%lwt () =
    match%lwt Cached_store.flush_writes cached_store with
    | Ok () ->
        Lwt.return_unit
    | Error e ->
        raise e
  in
  let%lwt new_commit = put_commit t new_mst.root ~previous:(Some prev_commit) in
  let new_commit_cid, new_commit_signed = new_commit in
  let commit_block =
    new_commit_signed |> signed_commit_to_yojson |> Dag_cbor.encode_yojson
  in
  let%lwt proof_blocks =
    Lwt_list.fold_left_s
      (fun acc ({path; _} : commit_evt_op) ->
        let%lwt key_proof =
          Cached_mst.proof_for_key new_mst new_mst.root path
        in
        Lwt.return (Block_map.merge acc key_proof) )
      Block_map.empty !commit_ops
  in
  let proof_blocks = Block_map.merge proof_blocks !added_leaves in
  let block_stream =
    proof_blocks |> Block_map.entries |> Lwt_seq.of_list
    |> Lwt_seq.cons (new_commit_cid, commit_block)
  in
  let%lwt blocks =
    Car.blocks_to_stream new_commit_cid block_stream |> Car.collect_stream
  in
  let%lwt ds = Data_store.connect () in
  let%lwt _ =
    Sequencer.sequence_commit ds ~did:t.did ~commit:new_commit_cid
      ~rev:new_commit_signed.rev ~blocks ~ops:!commit_ops ~since:prev_commit.rev
      ~prev_data:prev_commit.data ()
  in
  Lwt.return {commit= new_commit; results}

let load ?create ?(ensure_active = false) did : t Lwt.t =
  let%lwt ds_conn = Data_store.connect () in
  let%lwt user_db =
    try%lwt User_store.connect ?create did
    with _ ->
      Errors.invalid_request ~name:"RepoNotFound"
        "your princess is in another castle"
  in
  let%lwt actor =
    match%lwt Data_store.get_actor_by_identifier did ds_conn with
    | Some actor when ensure_active = false || actor.deactivated_at = None ->
        Lwt.return actor
    | Some _ ->
        Errors.invalid_request ~name:"RepoDeactivated"
          ("repository " ^ did ^ " is deactivated")
    | None ->
        failwith ("failed to retrieve actor for " ^ did)
  in
  let key = Kleidos.parse_multikey_str actor.signing_key in
  let%lwt commit = User_store.get_commit user_db in
  Lwt.return {key; did; actor; db= user_db; commit}

let export_car t : Car.stream Lwt.t =
  let%lwt root, commit =
    match%lwt User_store.get_commit t.db with
    | Some (r, c) ->
        Lwt.return (r, c)
    | None ->
        failwith ("failed to retrieve commit for " ^ t.did)
  in
  let mst : Mst.t = {blockstore= t.db; root= commit.data} in
  let commit_block =
    commit |> signed_commit_to_yojson |> Dag_cbor.encode_yojson
  in
  if Cid.create Dcbor commit_block <> root then
    failwith "commit does not match stored cid" ;
  (* the idea is to read ahead to collect record cids to reduce db calls to fetch records *)
  let batch_size = 100 in
  let ordered_stream = Mst.to_ordered_stream mst in
  (* read up to n items from stream, returning (items, remaining_stream) *)
  let rec read_ahead n stream acc =
    if n = 0 then Lwt.return (List.rev acc, stream)
    else
      match%lwt stream () with
      | Lwt_seq.Nil ->
          Lwt.return (List.rev acc, Lwt_seq.empty)
      | Lwt_seq.Cons (item, rest) ->
          read_ahead (n - 1) rest (item :: acc)
  in
  let leaf_cids_of items =
    List.filter_map (function Mist.Mst.Leaf cid -> Some cid | _ -> None) items
  in
  (* state: (buffer of items to yield, cache of fetched records, remaining stream) *)
  let blocks_stream : (Cid.t * bytes) Lwt_seq.t =
    let rec step (buffer, cache, stream) =
      match (buffer : Mist.Mst.ordered_item list) with
      | [] -> (
          (* buffer empty, try to read more *)
          let%lwt items, stream' = read_ahead batch_size stream [] in
          match items with
          | [] ->
              Lwt.return_none
          | _ ->
              (* batch fetch all leaf records in this chunk *)
              let leaf_cids = leaf_cids_of items in
              let%lwt records = User_store.get_records_by_cids t.db leaf_cids in
              let cache' =
                List.fold_left
                  (fun acc (cid, data) -> Block_map.set cid data acc)
                  cache records
              in
              step (items, cache', stream') )
      | Node (cid, data) :: rest ->
          Lwt.return_some ((cid, data), (rest, cache, stream))
      | Leaf cid :: rest -> (
        match Block_map.get cid cache with
        | Some data ->
            Lwt.return_some ((cid, data), (rest, cache, stream))
        | None -> (
          (* not in cache -> fetch individually *)
          match%lwt
            User_store.get_records_by_cids t.db [cid]
          with
          | (cid, data) :: _ ->
              Lwt.return_some ((cid, data), (rest, cache, stream))
          | [] ->
              (* record not found, skip *)
              step (rest, cache, stream) ) )
    in
    Lwt_seq.unfold_lwt step ([], Block_map.empty, ordered_stream)
  in
  let all_blocks = Lwt_seq.cons (root, commit_block) blocks_stream in
  Lwt.return @@ Car.blocks_to_stream root all_blocks

let import_car t (stream : Car.stream) : (t, exn) Lwt_result.t =
  let open Util.Syntax in
  let%lwt roots, blocks_seq = Car.read_car_stream stream in
  let root =
    match roots with [root] -> root | _ -> failwith "invalid number of roots"
  in
  try%lwt
    (* collect all blocks into a map *)
    let%lwt all_blocks =
      Lwt_seq.fold_left_s
        (fun acc (cid, block) -> Lwt.return (Block_map.set cid block acc))
        Block_map.empty blocks_seq
    in
    (* parse commit block *)
    let commit_bytes =
      match Block_map.get root all_blocks with
      | Some b ->
          b
      | None ->
          failwith "commit block not found in CAR"
    in
    let commit =
      match
        commit_bytes |> Dag_cbor.decode_to_yojson |> signed_commit_of_yojson
      with
      | Ok c ->
          c
      | Error e ->
          failwith ("invalid commit: " ^ e)
    in
    if commit.did <> t.did then failwith "did does not match commit did" ;
    let leaves = Mist.Mst.leaves_from_blocks all_blocks commit.data in
    let mst_node_cids =
      Mist.Mst.mst_node_cids_from_blocks all_blocks commit.data
    in
    (* collect mst node blocks for insert *)
    let mst_blocks =
      List.filter_map
        (fun cid ->
          match Block_map.get cid all_blocks with
          | Some block ->
              Some (cid, block)
          | None ->
              None )
        mst_node_cids
    in
    (* collect record data for insert *)
    let since = Tid.now () in
    let record_data, blob_refs =
      List.fold_left
        (fun (acc_data, acc_refs) (path, cid) ->
          match Block_map.get cid all_blocks with
          | Some data ->
              let record = Lex.of_cbor data in
              let record_refs =
                Util.find_blob_refs record
                |> List.map (fun (br : Mist.Blob_ref.t) -> (path, br.ref))
              in
              ( (path, cid, data, since) :: acc_data
              , List.rev_append record_refs acc_refs )
          | None ->
              failwith ("missing record block: " ^ Cid.to_string cid) )
        ([], []) leaves
    in
    let record_data = List.rev record_data in
    let%lwt _ =
      Util.use_pool t.db.db (fun conn ->
          Util.transact conn (fun () ->
              let$! _ = User_store.Queries.put_commit root commit_bytes conn in
              let$! () = User_store.Queries.clear_mst conn in
              let$! () = User_store.Bulk.put_blocks mst_blocks conn in
              let$! () =
                [%rapper execute {sql| DELETE FROM records |sql}] () conn
              in
              let$! () = User_store.Bulk.put_records record_data conn in
              let$! () = User_store.Bulk.put_blob_refs blob_refs conn in
              Lwt.return_ok () ) )
    in
    t.commit <- Some (root, commit) ;
    Lwt.return_ok t
  with exn -> Lwt.return_error exn

let rebuild_mst t : (Cid.t * signed_commit, exn) Lwt_result.t =
  try%lwt
    let%lwt record_cids = User_store.get_all_record_cids t.db in
    let record_count = List.length record_cids in
    Logs.info (fun m -> m "rebuilding MST from %d records" record_count) ;
    let%lwt () = User_store.clear_mst t.db in
    Logs.info (fun m -> m "cleared existing MST blocks") ;
    let%lwt mst_result = Mst.of_assoc t.db record_cids in
    Logs.info (fun m ->
        m "built new MST with root %s" (Cid.to_string mst_result.root) ) ;
    let%lwt prev_commit = User_store.get_commit t.db in
    let%lwt new_commit =
      put_commit t mst_result.root ~previous:(Option.map snd prev_commit)
    in
    let commit_cid, commit = new_commit in
    Logs.info (fun m ->
        m "inserted new commit %s with rev %s" (Cid.to_string commit_cid)
          commit.rev ) ;
    Lwt.return_ok new_commit
  with exn -> Lwt.return_error exn
