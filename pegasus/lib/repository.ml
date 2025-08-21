open Util.Exceptions
module Lex = Mist.Lex
module Mst = Mist.Mst.Make (User_store)
module StringMap = Lex.StringMap
module Tid = Mist.Tid

let cid_link_of_yojson = function
  | `Assoc link ->
      link |> List.assoc "$link" |> Cid.of_yojson
      |> Result.map (fun cid -> Some cid)
  | `Null ->
      Ok None
  | _ ->
      Error "commit prev not a valid cid"

let cid_link_to_yojson = function
  | Some cid ->
      Cid.to_yojson cid
  | None ->
      `Null

type commit =
  { did: string
  ; version: int (* always 3 *)
  ; data: Cid.t [@of_yojson Cid.of_yojson] [@to_yojson Cid.to_yojson]
  ; rev: Tid.t
  ; prev: Cid.t option
        [@of_yojson cid_link_of_yojson] [@to_yojson cid_link_to_yojson] }
[@@deriving yojson]

type signed_commit =
  { did: string
  ; version: int (* always 3 *)
  ; data: Cid.t [@of_yojson Cid.of_yojson] [@to_yojson Cid.to_yojson]
  ; rev: Tid.t
  ; prev: Cid.t option
        [@of_yojson cid_link_of_yojson] [@to_yojson cid_link_to_yojson]
  ; signature: bytes
        [@key "sig"]
        [@of_yojson
          fun x ->
            match Dag_cbor.of_yojson x with
            | `Bytes b ->
                Ok b
            | _ ->
                Error "commit sig not a valid bytes value"]
        [@to_yojson fun x -> Dag_cbor.to_yojson (`Bytes x)] }
[@@deriving yojson]

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
  ; db: Caqti_lwt.connection
  ; mutable block_map: Cid.t Mist.Mst.StringMap.t option }

let get_map t root =
  match t.block_map with
  | Some map ->
      Lwt.return map
  | None ->
      let%lwt map = Mst.build_map {blockstore= t.db; root} in
      t.block_map <- Some map ;
      Lwt.return map

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

let get_record_cid t 

let apply_writes (t : t) (commit_cid : Cid.t)
    ({did; data= mst_root; rev= commit_rev; _} : commit)
    (writes : repo_write list) (swap_commit : Cid.t option) : write_result Lwt.t
    =
  if swap_commit <> None && swap_commit <> Some commit_cid then
    raise
      (XrpcError
         ( "InvalidSwap"
         , Format.sprintf "swapRecord cid %s did not match commit cid %s"
             (Cid.to_string (Option.get swap_commit))
             (Cid.to_string commit_cid) ) ) ;
  let%lwt block_map = Lwt.map ref (get_map t mst_root) in
  let%lwt results =
    List.map
      (fun (w : repo_write) ->
        match w with
        | Create {collection; rkey; value; _} ->
            let rkey = Option.value rkey ~default:(Tid.now ()) in
            let path = Format.sprintf "%s/%s" collection rkey in
            let uri = Format.sprintf "at://%s/%s" did path in
            let%lwt () =
              match StringMap.find_opt path !block_map with
              | Some cid ->
                  raise
                    (XrpcError
                       ( "InvalidSwap"
                       , Format.sprintf
                           "attempted to write record %s that already exists \
                            with cid %s"
                           path (Cid.to_string cid) ) )
              | None ->
                  Lwt.return ()
            in
            let%lwt cid = User_store.put_record t.db (`LexMap value) path in
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
            let uri = Format.sprintf "at://%s/%s" did path in
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
                  (XrpcError
                     ( "InvalidSwap"
                     , Format.sprintf
                         "attempted to update record %s with cid %s" path
                         cid_str ) ) ) ;
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
            let%lwt new_cid = User_store.put_record t.db (`LexMap value) path in
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
                  (XrpcError
                     ( "InvalidSwap"
                     , Format.sprintf
                         "attempted to delete record %s with cid %s" path
                         cid_str ) ) ) ;
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
  let tid_now = Tid.now () in
  let rev =
    if tid_now > commit_rev then tid_now
    else
      try
        let ts, clockid = Tid.to_timestamp_us commit_rev in
        Tid.of_timestamp_us ~clockid (Int64.succ ts)
      with _ ->
        failwith
          (Format.sprintf
             "unable to produce commit rev greater than current %s; now is %s"
             commit_rev tid_now )
  in
  let commit = {version= 3; did; prev= None; rev; data= root} in
  let signed = sign_commit t commit in
  let commit_cid =
    signed |> signed_commit_to_yojson |> Dag_cbor.encode_yojson
    |> Cid.create Dcbor
  in
  Lwt.return {commit= (commit_cid, signed); results}
