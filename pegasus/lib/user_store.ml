open Lwt.Infix
open Util.Rapper
open Util.Syntax
module Block_map = Mist.Storage.Block_map
module Lex = Mist.Lex
module Tid = Mist.Tid

let delete_blob_file ~did ~cid ~storage =
  Lwt.async (fun () -> Blob_store.delete ~did ~cid ~storage)

module Types = struct
  open struct
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
  end

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

  type block = {cid: Cid.t; data: Blob.t}

  type record = {path: string; cid: Cid.t; value: Lex.repo_record; since: Tid.t}

  type blob = {cid: Cid.t; mimetype: string; storage: Blob_store.storage}

  type blob_with_contents =
    {cid: Cid.t; mimetype: string; data: Blob.t; storage: Blob_store.storage}
end

open Types

module Queries = struct
  (* mst block storage *)
  let get_block cid =
    [%rapper
      get_opt
        {sql| SELECT @CID{cid}, @Blob{data} FROM mst WHERE cid = %CID{cid} |sql}
        record_out]
      ~cid

  let get_blocks cids =
    [%rapper
      get_many
        {sql| SELECT @CID{cid}, @Blob{data} FROM mst WHERE cid IN (%list{%CID{cids}}) |sql}
        record_out]
      ~cids

  let has_block cid =
    [%rapper
      get_opt {sql| SELECT @CID{cid} FROM mst WHERE cid = %CID{cid} |sql}]
      ~cid

  let put_block cid block =
    [%rapper
      get_opt
        {sql| INSERT INTO mst (cid, data) VALUES (%CID{cid}, %Blob{block}) ON CONFLICT DO NOTHING RETURNING @CID{cid} |sql}]
      ~cid ~block

  let delete_block cid =
    [%rapper execute {sql| DELETE FROM mst WHERE cid = %CID{cid} |sql}] ~cid

  let delete_blocks cids =
    [%rapper
      get_many
        {sql| DELETE FROM mst WHERE cid IN (%list{%CID{cids}}) RETURNING @CID{cid} |sql}]
      ~cids

  let clear_mst = [%rapper execute {sql| DELETE FROM mst |sql}] ()

  (* mst misc *)
  let count_blocks =
    [%rapper get_one {sql| SELECT @int{COUNT(*)} FROM mst |sql}]

  (* repo commit *)
  let get_commit =
    [%rapper
      get_opt
        {sql| SELECT @CID{cid}, @Blob{data}
            FROM repo_commit WHERE id = 0
      |sql}]
      ()

  let put_commit cid data =
    [%rapper
      execute
        {sql| INSERT INTO repo_commit (id, cid, data)
            VALUES (0, %CID{cid}, %Blob{data})
            ON CONFLICT(id) DO UPDATE SET
              cid = excluded.cid,
              data = excluded.data
      |sql}]
      ~cid ~data

  (* record storage *)
  let get_record_cid =
    [%rapper
      get_opt
        {sql| SELECT @CID{cid} FROM records WHERE path = %string{path} |sql}]

  let get_all_record_cids =
    [%rapper get_many {sql| SELECT @string{path}, @CID{cid} FROM records |sql}]
      ()

  let get_record =
    [%rapper
      get_opt
        {sql| SELECT @CID{cid}, @Blob{data}, @string{since} FROM records WHERE path = %string{path} |sql}]

  let get_records_by_cids cids =
    [%rapper
      get_many
        {sql| SELECT @CID{cid}, @Blob{data} FROM records WHERE cid IN (%list{%CID{cids}}) |sql}
        record_out]
      ~cids

  let list_records =
    [%rapper
      get_many
        {sql| SELECT @string{path}, @CID{cid}, @Blob{data}, @string{since} FROM records
              WHERE path LIKE %string{collection} || '/' || '%'
              AND (since < %string{cursor} OR %string{cursor} = '')
              ORDER BY since DESC LIMIT %int{limit}
        |sql}]

  let count_records =
    [%rapper get_one {sql| SELECT @int{COUNT(*)} FROM records |sql}]

  let list_records_reverse =
    [%rapper
      get_many
        {sql| SELECT @string{path}, @CID{cid}, @Blob{data}, @string{since} FROM records
              WHERE path LIKE %string{collection} || '/' || '%'
              AND (since > %string{cursor} OR %string{cursor} = '')
        	  ORDER BY since ASC LIMIT %int{limit}
        |sql}]

  let put_record =
    [%rapper
      execute
        {sql| INSERT INTO records (path, cid, data, since)
              VALUES (%string{path}, %CID{cid}, %Blob{data}, %string{since})
              ON CONFLICT (path) DO UPDATE SET cid = excluded.cid, data = excluded.data, since = excluded.since
        |sql}]

  let delete_record path =
    [%rapper execute {sql| DELETE FROM records WHERE path = %string{path} |sql}]
      ~path

  let get_blob =
    [%rapper
      get_opt
        {sql| SELECT @CID{cid}, @string{mimetype}, @string{storage} FROM blobs WHERE cid = %CID{cid} |sql}]

  let list_blobs =
    [%rapper
      get_many
        {sql| SELECT @CID{cid} FROM blobs WHERE cid > %string{cursor} ORDER BY cid LIMIT %int{limit} |sql}]

  let list_blobs_since =
    [%rapper
      get_many
        {sql|
          SELECT @CID{cid}
          FROM blobs
          WHERE cid > %string{cursor}
            AND (
              SELECT MIN(records.since)
              FROM blobs_records
              JOIN records ON records.path = blobs_records.record_path
              WHERE blobs_records.blob_cid = blobs.cid
            ) > %string{since}
          ORDER BY cid
          LIMIT %int{limit}
        |sql}]

  let list_missing_blobs =
    [%rapper
      get_many
        {sql|
          SELECT @string{record_path}, @CID{blob_cid}
          FROM blobs_records
          WHERE NOT EXISTS (
            SELECT 1 FROM blobs WHERE cid = blobs_records.blob_cid
          )
          AND blob_cid > %string{cursor}
          ORDER BY blob_cid
          LIMIT %int{limit}
        |sql}]

  let count_blobs =
    [%rapper get_one {sql| SELECT @int{COUNT(*)} FROM blobs |sql}]

  let count_referenced_blobs =
    [%rapper
      get_one
        {sql| SELECT @int{COUNT(*)} FROM blobs WHERE cid IN (
                SELECT blob_cid FROM blobs_records
              )
        |sql}]

  let put_blob cid mimetype storage =
    [%rapper
      get_one
        {sql| INSERT INTO blobs (cid, mimetype, storage)
              VALUES (%CID{cid}, %string{mimetype}, %string{storage})
              ON CONFLICT (cid) DO UPDATE SET mimetype = excluded.mimetype, storage = excluded.storage
              RETURNING @CID{cid}
        |sql}]
      ~cid ~mimetype ~storage

  let delete_blob cid =
    [%rapper execute {sql| DELETE FROM blobs WHERE cid = %CID{cid} |sql}] ~cid

  let update_blob_storage cid storage =
    [%rapper
      execute
        {sql| UPDATE blobs SET storage = %string{storage} WHERE cid = %CID{cid} |sql}]
      ~cid ~storage

  let list_blobs_by_storage =
    [%rapper
      get_many
        {sql| SELECT @CID{cid}, @string{mimetype} FROM blobs
              WHERE storage = %string{storage}
              AND cid > %string{cursor}
              ORDER BY cid
              LIMIT %int{limit}
        |sql}]

  let delete_orphaned_blobs_by_record_path path =
    [%rapper
      get_many
        {sql| DELETE FROM blobs
              WHERE cid IN (
                  SELECT blob_cid FROM blobs_records WHERE record_path = %string{path}
              )
              AND NOT EXISTS (
                  SELECT 1 FROM blobs_records
                  WHERE blob_cid = blobs.cid AND record_path != %string{path}
              )
              RETURNING @CID{cid}, @string{storage}
        |sql}]
      ~path

  let list_blob_refs path =
    [%rapper
      get_many
        {sql| SELECT @CID{cid} FROM blobs WHERE path LIKE %string{path} |sql}]
      ~path

  let put_blob_ref cid path =
    [%rapper
      execute
        {sql| INSERT INTO blobs_records (blob_cid, record_path) VALUES (
                %CID{cid},
                %string{path}
              )
              ON CONFLICT DO NOTHING
        |sql}]
      ~cid ~path

  let clear_blob_refs path cids =
    [%rapper
      execute
        {sql| DELETE FROM blobs_records
              WHERE record_path LIKE %string{path}
              AND blob_cid IN (
                SELECT cid FROM blobs WHERE cid IN (%list{%CID{cids}})
              )
        |sql}]
      ~path ~cids
end

type t = {did: string; db: Util.caqti_pool}

let pool_cache : (string, t) Hashtbl.t = Hashtbl.create 64

let pool_cache_mutex = Lwt_mutex.create ()

let connect ?create did : t Lwt.t =
  Lwt_mutex.with_lock pool_cache_mutex (fun () ->
      match Hashtbl.find_opt pool_cache did with
      | Some cached ->
          Lwt.return cached
      | None ->
          let%lwt db =
            Util.connect_sqlite ?create ~write:true
              (Util.Constants.user_db_location did)
          in
          let%lwt () = Migrations.run_migrations User_store db in
          let t = {did; db} in
          Hashtbl.replace pool_cache did t ;
          Lwt.return t )

(* mst blocks; implements Writable_blockstore *)

let get_bytes t cid : Blob.t option Lwt.t =
  Util.use_pool t.db @@ Queries.get_block cid
  >|= function Some {data; _} -> Some data | None -> None

let get_blocks t cids : Block_map.with_missing Lwt.t =
  if List.is_empty cids then
    Lwt.return ({blocks= Block_map.empty; missing= []} : Block_map.with_missing)
  else
    let%lwt blocks = Util.use_pool t.db @@ Queries.get_blocks cids in
    Lwt.return
      (List.fold_left
         (fun (acc : Block_map.with_missing) cid ->
           match List.find_opt (fun (b : block) -> b.cid = cid) blocks with
           | Some {data; _} ->
               {acc with blocks= Block_map.set cid data acc.blocks}
           | None ->
               {acc with missing= cid :: acc.missing} )
         {blocks= Block_map.empty; missing= []}
         cids )

let has t cid : bool Lwt.t =
  Util.use_pool t.db @@ Queries.has_block cid
  >|= function Some _ -> true | None -> false

let put_block t cid block : (bool, exn) Lwt_result.t =
  Lwt_result.catch
  @@ fun () ->
  match%lwt Util.use_pool t.db @@ Queries.put_block cid block with
  | Some _ ->
      Lwt.return true
  | None ->
      Lwt.return false

let put_many t bm : (int, exn) Lwt_result.t =
  Util.multi_query t.db
    (List.map
       (fun (cid, block) -> Queries.put_block cid block)
       (Block_map.entries bm) )

let delete_block t cid : (bool, exn) Lwt_result.t =
  Lwt_result.catch
  @@ fun () -> Util.use_pool t.db @@ Queries.delete_block cid >|= fun _ -> true

let delete_many t cids : (int, exn) Lwt_result.t =
  Lwt_result.catch
  @@ fun () -> Util.use_pool t.db @@ Queries.delete_blocks cids >|= List.length

let clear_mst t : unit Lwt.t =
  let%lwt () = Util.use_pool t.db Queries.clear_mst in
  Lwt.return_unit

(* mst misc *)

let count_blocks t : int Lwt.t = Util.use_pool t.db @@ Queries.count_blocks ()

(* repo commit *)

let get_commit t : (Cid.t * signed_commit) option Lwt.t =
  let%lwt commit = Util.use_pool t.db Queries.get_commit in
  Lwt.return
  @@ Option.map
       (fun (cid, data) ->
         ( cid
         , data |> Dag_cbor.decode_to_yojson |> signed_commit_of_yojson
           |> Result.get_ok ) )
       commit

let put_commit t commit : (Cid.t, exn) Lwt_result.t =
  let data = commit |> signed_commit_to_yojson |> Dag_cbor.encode_yojson in
  let cid = Cid.create Dcbor data in
  ( Lwt_result.catch
  @@ fun () -> Util.use_pool t.db @@ Queries.put_commit cid data )
  |> Lwt_result.map (fun () -> cid)

(* records *)

let get_record t path : record option Lwt.t =
  Util.use_pool t.db @@ Queries.get_record ~path
  >|= Option.map (fun (cid, data, since) ->
      {path; cid; value= Lex.of_cbor data; since} )

let get_record_cid t path : Cid.t option Lwt.t =
  Util.use_pool t.db @@ Queries.get_record_cid ~path

let get_all_record_cids t : (string * Cid.t) list Lwt.t =
  Util.use_pool t.db Queries.get_all_record_cids

let get_records_by_cids t cids : (Cid.t * Blob.t) list Lwt.t =
  if List.is_empty cids then Lwt.return []
  else
    Util.use_pool t.db @@ Queries.get_records_by_cids cids
    >|= List.map (fun ({cid; data} : block) -> (cid, data))

let list_records t ?(limit = 100) ?(cursor = "") ?(reverse = false) collection :
    record list Lwt.t =
  let fn =
    if reverse then Queries.list_records_reverse else Queries.list_records
  in
  Util.use_pool t.db @@ fn ~collection ~limit ~cursor
  >|= List.map (fun (path, cid, data, since) ->
      {path; cid; value= Lex.of_cbor data; since} )

let count_records t : int Lwt.t = Util.use_pool t.db @@ Queries.count_records ()

let put_record t record path : (Cid.t * bytes) Lwt.t =
  let cid, data = Lex.to_cbor_block record in
  let since = Tid.now () in
  let%lwt () =
    Util.use_pool t.db @@ Queries.put_record ~path ~cid ~data ~since
  in
  Lwt.return (cid, data)

let put_record_raw t ~path ~cid ~data ~since : unit Lwt.t =
  Util.use_pool t.db @@ Queries.put_record ~path ~cid ~data ~since

let delete_record t path : unit Lwt.t =
  Util.use_pool t.db (fun conn ->
      Util.transact conn (fun () ->
          let del = Queries.delete_record path conn in
          let$! () = del in
          let$! deleted_blobs =
            Queries.delete_orphaned_blobs_by_record_path path conn
          in
          let () =
            List.iter
              (fun (cid, storage_str) ->
                let storage = Blob_store.storage_of_string storage_str in
                delete_blob_file ~did:t.did ~cid ~storage )
              deleted_blobs
          in
          del ) )

(* blobs *)

let get_blob t cid : blob_with_contents option Lwt.t =
  match%lwt Util.use_pool t.db @@ Queries.get_blob ~cid with
  | None ->
      Lwt.return_none
  | Some (cid, mimetype, storage_str) -> (
      let storage = Blob_store.storage_of_string storage_str in
      let%lwt data_opt = Blob_store.get ~did:t.did ~cid ~storage in
      match data_opt with
      | Some data ->
          Lwt.return_some {cid; mimetype; data; storage}
      | None ->
          Lwt.return_none )

let get_blob_metadata t cid : blob option Lwt.t =
  match%lwt Util.use_pool t.db @@ Queries.get_blob ~cid with
  | None ->
      Lwt.return_none
  | Some (cid, mimetype, storage_str) ->
      let storage = Blob_store.storage_of_string storage_str in
      Lwt.return_some {cid; mimetype; storage}

let list_blobs ?since t ~limit ~cursor : Cid.t list Lwt.t =
  Util.use_pool t.db
  @@
  match since with
  | Some since ->
      Queries.list_blobs_since ~limit ~cursor ~since
  | None ->
      Queries.list_blobs ~limit ~cursor

let list_missing_blobs ?(limit = 500) ?(cursor = "") t :
    (string * Cid.t) list Lwt.t =
  Util.use_pool t.db @@ Queries.list_missing_blobs ~limit ~cursor

let count_blobs t : int Lwt.t = Util.use_pool t.db @@ Queries.count_blobs ()

let count_referenced_blobs t : int Lwt.t =
  Util.use_pool t.db @@ Queries.count_referenced_blobs ()

let put_blob t cid mimetype data : Cid.t Lwt.t =
  let%lwt storage = Blob_store.put ~did:t.did ~cid ~data in
  let storage_str = Blob_store.storage_to_string storage in
  Util.use_pool t.db @@ Queries.put_blob cid mimetype storage_str

let delete_blob t cid : unit Lwt.t =
  let%lwt blob_opt = get_blob_metadata t cid in
  ( match blob_opt with
  | Some {storage; _} ->
      delete_blob_file ~did:t.did ~cid ~storage
  | None ->
      () ) ;
  Util.use_pool t.db @@ Queries.delete_blob cid

let delete_orphaned_blobs_by_record_path t path :
    (Cid.t * Blob_store.storage) list Lwt.t =
  let%lwt results =
    Util.use_pool t.db @@ Queries.delete_orphaned_blobs_by_record_path path
  in
  Lwt.return
  @@ List.map
       (fun (cid, storage_str) ->
         (cid, Blob_store.storage_of_string storage_str) )
       results

let list_blob_refs t path : Cid.t list Lwt.t =
  Util.use_pool t.db @@ Queries.list_blob_refs path

let put_blob_ref t path cid : unit Lwt.t =
  Util.use_pool t.db @@ Queries.put_blob_ref path cid

let put_blob_refs t path cids : (unit, exn) Lwt_result.t =
  if List.is_empty cids then Lwt.return_ok ()
  else
    Lwt_result.map (fun _ -> ())
    @@ Util.multi_query t.db
         (List.map (fun cid -> Queries.put_blob_ref cid path) cids)

let clear_blob_refs t path cids : unit Lwt.t =
  if List.is_empty cids then Lwt.return_unit
  else Util.use_pool t.db @@ Queries.clear_blob_refs path cids

let update_blob_storage t cid storage : unit Lwt.t =
  let storage_str = Blob_store.storage_to_string storage in
  Util.use_pool t.db @@ Queries.update_blob_storage cid storage_str

let list_blobs_by_storage t ~storage ~limit ~cursor :
    (Cid.t * string) list Lwt.t =
  let storage_str = Blob_store.storage_to_string storage in
  Util.use_pool t.db
  @@ Queries.list_blobs_by_storage ~storage:storage_str ~limit ~cursor

module Bulk = struct
  open struct
    let escape_sql_string s = Str.global_replace (Str.regexp "'") "''" s

    let bytes_to_hex data =
      let buf = Buffer.create (Bytes.length data * 2) in
      Bytes.iter
        (fun c -> Buffer.add_string buf (Printf.sprintf "%02x" (Char.code c)))
        data ;
      Buffer.contents buf

    let chunk_list n lst =
      if n <= 0 then invalid_arg "negative n passed to chunk_list" ;
      let rec take_n acc remaining xs =
        match (remaining, xs) with
        | _, [] ->
            (List.rev acc, [])
        | 0, rest ->
            (List.rev acc, rest)
        | _, x :: xs' ->
            take_n (x :: acc) (remaining - 1) xs'
      in
      let rec go xs =
        match xs with
        | [] ->
            []
        | _ ->
            let chunk, rest = take_n [] n xs in
            chunk :: go rest
      in
      go lst
  end

  let put_blocks (blocks : (Cid.t * bytes) list) conn =
    if List.is_empty blocks then Lwt.return_ok ()
    else
      let module C = (val conn : Caqti_lwt.CONNECTION) in
      let chunks = chunk_list 200 blocks in
      let rec process_chunks = function
        | [] ->
            Lwt.return_ok ()
        | chunk :: rest -> (
            let values =
              List.map
                (fun (cid, data) ->
                  let cid_str = escape_sql_string (Cid.to_string cid) in
                  let hex_data = bytes_to_hex data in
                  Printf.sprintf "('%s', CAST(X'%s' AS TEXT))" cid_str hex_data )
                chunk
              |> String.concat ", "
            in
            let sql =
              Printf.sprintf
                "INSERT INTO mst (cid, data) VALUES %s ON CONFLICT DO NOTHING"
                values
            in
            let query =
              Caqti_request.Infix.( ->. ) Caqti_type.unit Caqti_type.unit sql
            in
            let%lwt result = C.exec query () in
            match result with
            | Ok () ->
                process_chunks rest
            | Error e ->
                Lwt.return_error e )
      in
      process_chunks chunks

  let put_records (records : (string * Cid.t * bytes * string) list) conn =
    if List.is_empty records then Lwt.return_ok ()
    else
      let module C = (val conn : Caqti_lwt.CONNECTION) in
      let chunks = chunk_list 100 records in
      let rec process_chunks = function
        | [] ->
            Lwt.return_ok ()
        | chunk :: rest -> (
            let values =
              List.map
                (fun (path, cid, data, since) ->
                  let hex_data = bytes_to_hex data in
                  Printf.sprintf "('%s', '%s', CAST(X'%s' AS TEXT), '%s')"
                    (escape_sql_string path)
                    (escape_sql_string (Cid.to_string cid))
                    hex_data (escape_sql_string since) )
                chunk
              |> String.concat ", "
            in
            let sql =
              Printf.sprintf
                "INSERT INTO records (path, cid, data, since) VALUES %s ON \
                 CONFLICT (path) DO UPDATE SET cid = excluded.cid, data = \
                 excluded.data, since = excluded.since"
                values
            in
            let query =
              Caqti_request.Infix.( ->. ) Caqti_type.unit Caqti_type.unit sql
            in
            let%lwt result = C.exec query () in
            match result with
            | Ok () ->
                process_chunks rest
            | Error e ->
                Lwt.return_error e )
      in
      process_chunks chunks

  let put_blob_refs (refs : (string * Cid.t) list) conn =
    if List.is_empty refs then Lwt.return_ok ()
    else
      let module C = (val conn : Caqti_lwt.CONNECTION) in
      let chunks = chunk_list 200 refs in
      let rec process_chunks = function
        | [] ->
            Lwt.return_ok ()
        | chunk :: rest -> (
            let values =
              List.map
                (fun (path, cid) ->
                  Printf.sprintf "('%s', '%s')" (escape_sql_string path)
                    (escape_sql_string (Cid.to_string cid)) )
                chunk
              |> String.concat ", "
            in
            let sql =
              Printf.sprintf
                "INSERT INTO blobs_records (record_path, blob_cid) VALUES %s \
                 ON CONFLICT DO NOTHING"
                values
            in
            let query =
              Caqti_request.Infix.( ->. ) Caqti_type.unit Caqti_type.unit sql
            in
            let%lwt result = C.exec query () in
            match result with
            | Ok () ->
                process_chunks rest
            | Error e ->
                Lwt.return_error e )
      in
      process_chunks chunks
end
