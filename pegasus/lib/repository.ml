open User_store.Types
open Util.Exceptions
module Lex = Mist.Lex
module Mst = Mist.Mst.Make (User_store)
module StringMap = Lex.StringMap
module Tid = Mist.Tid

type signing_key = P256 of bytes | K256 of bytes

type repo_write =
  | Create of
      { type': string [@key "$type"]
      ; collection: string
      ; rkey: string option
      ; value: Lex.repo_record }
  | Update of
      { type': string [@key "$type"]
      ; collection: string
      ; rkey: string
      ; value: Lex.repo_record
      ; swap_record: Cid.t option [@key "swapRecord"] }
  | Delete of
      { type': string [@key "$type"]
      ; collection: string
      ; rkey: string
      ; swap_record: Cid.t option [@key "swapRecord"] }
[@@deriving yojson]

let repo_write_of_yojson (json : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  let type' = member "$type" json |> to_string in
  let collection = member "collection" json |> to_string in
  let rkey = match member "rkey" json with `String s -> Some s | _ -> None in
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
      Create {type'; collection; rkey; value}
  | "com.atproto.repo.applyWrites#update" ->
      let value =
        member "value" json |> Lex.repo_record_of_yojson |> Result.get_ok
      in
      Update {type'; collection; rkey= Option.get rkey; value; swap_record}
  | "com.atproto.repo.applyWrites#delete" ->
      Delete {type'; collection; rkey= Option.get rkey; swap_record}
  | _ ->
      raise (Invalid_argument "invalid applyWrites write $type")

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
  match type' with
  | "com.atproto.repo.applyWrites#createResult" ->
      let uri = member "uri" json |> to_string in
      let cid =
        member "cid" json |> to_string |> Cid.of_string |> Result.get_ok
      in
      Create {type'; uri; cid}
  | "com.atproto.repo.applyWrites#updateResult" ->
      let uri = member "uri" json |> to_string in
      let cid =
        member "cid" json |> to_string |> Cid.of_string |> Result.get_ok
      in
      Update {type'; uri; cid}
  | "com.atproto.repo.applyWrites#deleteResult" ->
      Delete {type'}
  | _ ->
      failwith "invalid applyWrites result $type"

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
  { key: signing_key
  ; did: string
  ; db: Caqti_lwt.connection
  ; mutable block_map: Cid.t StringMap.t option
  ; mutable commit: Cid.t option }

let get_map t : Cid.t StringMap.t Lwt.t =
  let%lwt root, commit =
    match%lwt User_store.get_commit t.db with
    | Some (r, c) ->
        Lwt.return (r, c)
    | None ->
        failwith ("failed to retrieve commit for " ^ t.did)
  in
  if t.commit <> Some root then t.commit <- Some root ;
  match t.block_map with
  | Some map ->
      Lwt.return map
  | _ ->
      let%lwt map = Mst.build_map {blockstore= t.db; root= commit.data} in
      t.block_map <- Some map ;
      Lwt.return map

let get_record_cid t path : Cid.t option Lwt.t =
  let%lwt map = get_map t in
  Lwt.return @@ StringMap.find_opt path map

let get_record t path : record option Lwt.t =
  User_store.get_record_by_path t.db path

let list_collections t : string list Lwt.t =
  let module Set = Set.Make (String) in
  let%lwt map = get_map t in
  StringMap.bindings map
  |> List.fold_left
       (fun (acc : Set.t) (path, _) ->
         let collection = String.split_on_char '/' path |> List.hd in
         Set.add collection acc )
       Set.empty
  |> Set.to_list |> Lwt.return

let list_records t collection : (string * Cid.t * record) list Lwt.t =
  let%lwt map = get_map t in
  StringMap.bindings map
  |> List.filter (fun (path, _) ->
         String.starts_with ~prefix:(path ^ "/") collection )
  |> Lwt_list.fold_left_s
       (fun acc (path, cid) ->
         match%lwt User_store.get_record_by_cid t.db cid with
         | Some record ->
             Lwt.return
               ((Format.sprintf "at://%s/%s" t.did path, cid, record) :: acc)
         | None ->
             Lwt.return acc )
       []

let sign_commit t commit : signed_commit =
  let sign_fn, privkey =
    match t.key with
    | K256 k ->
        (Kleidos.K256.sign, k)
    | P256 k ->
        (Kleidos.P256.sign, k)
  in
  let msg = commit |> commit_to_yojson |> Dag_cbor.encode_yojson in
  let signature = sign_fn ~privkey ~msg in
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
  t.commit <- Some commit_cid ;
  Lwt.return (commit_cid, signed)

let put_initial_commit t : (Cid.t * signed_commit) Lwt.t =
  let%lwt commit = User_store.get_commit t.db in
  if commit <> None then failwith ("commit already exists for " ^ t.did) ;
  let%lwt {root; _} = Mst.create_empty t.db |> Lwt_result.get_exn in
  put_commit t root

let apply_writes (t : t) (writes : repo_write list) (swap_commit : Cid.t option)
    : write_result Lwt.t =
  let%lwt commit =
    match%lwt User_store.get_commit t.db with
    | Some (_, commit) ->
        Lwt.return commit
    | None ->
        failwith ("failed to retrieve commit for " ^ t.did)
  in
  if swap_commit <> None && swap_commit <> t.commit then
    raise
      (Errors.invalid_request ~name:"InvalidSwap"
         (Format.sprintf "swapCommit cid %s did not match last commit cid %s"
            (Cid.to_string (Option.get swap_commit))
            (match t.commit with Some c -> Cid.to_string c | None -> "null") ) ) ;
  let%lwt block_map = Lwt.map ref (get_map t) in
  let%lwt results =
    List.map
      (fun (w : repo_write) ->
        match w with
        | Create {collection; rkey; value; _} ->
            let rkey = Option.value rkey ~default:(Tid.now ()) in
            let path = Format.sprintf "%s/%s" collection rkey in
            let uri = Format.sprintf "at://%s/%s" t.did path in
            let%lwt () =
              match StringMap.find_opt path !block_map with
              | Some cid ->
                  raise
                    (Errors.invalid_request ~name:"InvalidSwap"
                       (Format.sprintf
                          "attempted to write record %s that already exists \
                           with cid %s"
                          path (Cid.to_string cid) ) )
              | None ->
                  Lwt.return ()
            in
            let record_with_type : Lex.repo_record =
              if StringMap.mem "$type" value then value
              else StringMap.add "$type" (`String collection) value
            in
            let%lwt cid =
              User_store.put_record t.db (`LexMap record_with_type) path
            in
            block_map := StringMap.add path cid !block_map ;
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
            let old_cid = StringMap.find_opt path !block_map in
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
                raise
                  (Errors.invalid_request ~name:"InvalidSwap"
                     (Format.sprintf "attempted to update record %s with cid %s"
                        path cid_str ) ) ) ;
            let%lwt () =
              match old_cid with
              | Some _ -> (
                  match%lwt User_store.get_record_by_path t.db path with
                  | Some record ->
                      let refs =
                        Util.find_blob_refs record.value
                        |> List.map (fun (r : Mist.Blob_ref.t) -> r.ref)
                      in
                      let%lwt () = User_store.clear_blob_refs t.db path refs in
                      Lwt.return_unit
                  | None ->
                      Lwt.return_unit )
              | None ->
                  Lwt.return_unit
            in
            let record_with_type : Lex.repo_record =
              if StringMap.mem "$type" value then value
              else StringMap.add "$type" (`String collection) value
            in
            let%lwt new_cid =
              User_store.put_record t.db (`LexMap record_with_type) path
            in
            block_map := StringMap.add path new_cid !block_map ;
            Lwt.return
              (Update
                 { type'= "com.atproto.repo.applyWrites#updateResult"
                 ; uri
                 ; cid= new_cid } )
        | Delete {collection; rkey; swap_record; _} ->
            let path = Format.sprintf "%s/%s" collection rkey in
            let cid = StringMap.find_opt path !block_map in
            ( if cid = None || (swap_record <> None && swap_record <> cid) then
                let cid_str =
                  match cid with
                  | Some cid ->
                      Cid.to_string cid
                  | None ->
                      "null"
                in
                raise
                  (Errors.invalid_request ~name:"InvalidSwap"
                     (Format.sprintf "attempted to delete record %s with cid %s"
                        path cid_str ) ) ) ;
            let%lwt () =
              match%lwt User_store.get_record_by_path t.db path with
              | Some record ->
                  let refs =
                    Util.find_blob_refs record.value
                    |> List.map (fun (r : Mist.Blob_ref.t) -> r.ref)
                  in
                  let%lwt () = User_store.clear_blob_refs t.db path refs in
                  Lwt.return_unit
              | None ->
                  Lwt.return_unit
            in
            block_map := StringMap.remove path !block_map ;
            Lwt.return
              (Delete {type'= "com.atproto.repo.applyWrites#deleteResult"}) )
      writes
    |> Lwt.all
  in
  let%lwt () = User_store.clear_mst t.db in
  let%lwt {root; _} = Mst.of_assoc t.db (StringMap.bindings !block_map) in
  let%lwt commit = put_commit t root ~previous:(Some commit) in
  Lwt.return {commit; results}

let load did : t Lwt.t =
  let%lwt data_store_conn =
    Util.connect_sqlite Util.Constants.pegasus_db_location
  in
  let%lwt user_db = Util.connect_sqlite (Util.Constants.user_db_location did) in
  let%lwt () = User_store.init user_db in
  let%lwt {signing_key; _} =
    match%lwt Data_store.get_actor_by_identifier did data_store_conn with
    | Some actor ->
        Lwt.return actor
    | None ->
        failwith ("failed to retrieve actor for " ^ did)
  in
  let key =
    match Kleidos.parse_multikey_str signing_key with
    | key, (module M) when M.name = "K256" ->
        K256 key
    | key, (module M) when M.name = "P256" ->
        P256 key
    | _ ->
        failwith "unsupported key type"
  in
  let%lwt commit =
    match%lwt User_store.get_commit user_db with
    | Some (cid, _) ->
        Lwt.return_some cid
    | None ->
        Lwt.return_none
  in
  Lwt.return {key; did; db= user_db; block_map= None; commit}

let export_car t : Car.stream Lwt.t =
  let%lwt root, commit =
    match%lwt User_store.get_commit t.db with
    | Some (r, c) ->
        Lwt.return (r, c)
    | None ->
        failwith ("failed to retrieve commit for " ^ t.did)
  in
  let mst : Mst.t = {blockstore= t.db; root= commit.data} in
  let mst_blocks = Mst.to_blocks_stream mst in
  let commit_block =
    commit |> signed_commit_to_yojson |> Dag_cbor.encode_yojson
  in
  if Cid.create Dcbor commit_block <> root then
    failwith "commit does not match stored cid" ;
  Lwt.return
  @@ Car.blocks_to_stream root (Lwt_seq.cons (root, commit_block) mst_blocks)

let import_car (did : string) (stream : Car.stream) : t Lwt.t =
  let%lwt t = load did in
  let%lwt roots, blocks = Car.read_car_stream stream in
  let root =
    match roots with [root] -> root | _ -> failwith "invalid number of roots"
  in
  let%lwt () =
    Lwt_seq.iter_s
      (fun (cid, block) ->
        if cid = root then (
          let commit =
            block |> Dag_cbor.decode_to_yojson |> signed_commit_of_yojson
            |> Result.get_ok
          in
          if commit.did <> did then failwith "did does not match commit did" ;
          let%lwt _ = Lwt_result.get_exn @@ User_store.put_commit t.db commit in
          Lwt.return_unit )
        else
          let%lwt _ =
            Lwt_result.get_exn @@ User_store.put_block t.db cid block
          in
          Lwt.return_unit )
      blocks
  in
  Lwt.return t
