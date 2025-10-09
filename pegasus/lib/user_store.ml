open Lwt.Infix
open Util.Rapper
open Util.Syntax
module Block_map = Mist.Storage.Block_map
module Lex = Mist.Lex
module Tid = Mist.Tid

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

  type blob = {id: int; cid: Cid.t; mimetype: string}

  type blob_with_contents = {id: int; cid: Cid.t; mimetype: string; data: Blob.t}
end

open Types

module Queries = struct
  (* mst block storage *)
  let create_blocks_tables conn =
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS mst (
                  cid TEXT NOT NULL PRIMARY KEY,
                  data BLOB NOT NULL
                )
          |sql}]
        () conn
    in
    [%rapper
      execute
        {sql| CREATE TABLE IF NOT EXISTS repo_commit (
                id INTEGER PRIMARY KEY CHECK (id = 0),
                cid TEXT NOT NULL,
                data BLOB NOT NULL
              )
        |sql}]
      () conn

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
  let create_records_table conn =
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS records (
                path TEXT NOT NULL PRIMARY KEY,
                cid TEXT NOT NULL,
                since TEXT NOT NULL,
                data BLOB NOT NULL
              )
        |sql}]
        () conn
    in
    [%rapper
      execute
        {sql| CREATE INDEX IF NOT EXISTS records_cid_idx ON records (cid);
              CREATE INDEX IF NOT EXISTS records_since_idx ON records (since);
        |sql}]
      () conn

  let get_record_by_path =
    [%rapper
      get_opt
        {sql| SELECT @CID{cid}, @Blob{data}, @string{since} FROM records WHERE path = %string{path} |sql}]

  let get_record_by_cid =
    [%rapper
      get_opt
        {sql| SELECT @string{path}, @Blob{data}, @string{since} FROM records WHERE cid = %CID{cid} |sql}]

  let list_records =
    [%rapper
      get_many
        {sql| SELECT @string{path}, @CID{cid}, @Blob{data}, @string{since} FROM records
              WHERE path LIKE concat(%string{collection}, '/', '%')
              AND (since < %string{cursor} OR %string{cursor} = '')
              ORDER BY since DESC LIMIT %int{limit}
        |sql}]

  let list_records_reverse =
    [%rapper
      get_many
        {sql| SELECT @string{path}, @CID{cid}, @Blob{data}, @string{since} FROM records
              WHERE path LIKE concat(%string{collection}, '/', '%')
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

  (* blob storage *)
  let create_blobs_tables conn =
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS blobs (
                id INTEGER PRIMARY KEY,
                cid TEXT NOT NULL,
                mimetype TEXT NOT NULL
              )
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS blobs_records (
                  blob_id INTEGER NOT NULL REFERENCES blobs(id) ON DELETE CASCADE,
                  record_path TEXT NOT NULL REFERENCES records(path) ON DELETE CASCADE,
                  PRIMARY KEY (blob_id, record_path)
                )
          |sql}]
        () conn
    in
    [%rapper
      execute
        {sql| CREATE TRIGGER IF NOT EXISTS cleanup_orphaned_blobs
                AFTER DELETE ON blobs_records
                BEGIN
                  DELETE FROM blobs
                  WHERE id NOT IN (
                    SELECT DISTINCT blob_id FROM blobs_records
                  );
                END
        |sql}
        syntax_off]
      () conn

  let get_blob =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @CID{cid}, @string{mimetype} FROM blobs WHERE cid = %CID{cid} |sql}
        record_out]

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
              WHERE blobs_records.blob_id = blobs.id
            ) > %string{since}
          ORDER BY cid
          LIMIT %int{limit}
        |sql}]

  let put_blob cid mimetype =
    [%rapper
      get_one
        {sql| INSERT INTO blobs (cid, mimetype)
              VALUES (%CID{cid}, %string{mimetype})
              ON CONFLICT (cid) DO UPDATE SET mimetype = excluded.mimetype
              RETURNING @int{id}
        |sql}]
      ~cid ~mimetype

  let list_blob_refs path =
    [%rapper
      get_many
        {sql| SELECT @CID{cid} FROM blobs WHERE path LIKE %string{path} |sql}]
      ~path

  let put_blob_ref cid path =
    [%rapper
      execute
        {sql| INSERT INTO blobs_records (blob_id, record_path) VALUES (
                (SELECT id FROM blobs WHERE cid = %CID{cid} LIMIT 1),
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
              AND blob_id IN (
                SELECT id FROM blobs WHERE cid IN (%list{%CID{cids}})
              )
        |sql}]
      ~path ~cids
end

type t = {did: string; db: Util.caqti_pool}

let connect ?create ?write did : t Lwt.t =
  let%lwt db =
    Util.connect_sqlite ?create ?write (Util.Constants.user_db_location did)
  in
  Lwt.return {did; db}

let init t : unit Lwt.t =
  let%lwt () = Util.use_pool t.db Queries.create_blocks_tables in
  let%lwt () = Util.use_pool t.db Queries.create_records_table in
  let%lwt () = Util.use_pool t.db Queries.create_blobs_tables in
  Lwt.return_unit

(* mst blocks; implements Writable_blockstore *)

let get_bytes t cid : Blob.t option Lwt.t =
  Util.use_pool t.db @@ Queries.get_block cid
  >|= function Some {data; _} -> Some data | None -> None

let get_blocks t cids : Block_map.with_missing Lwt.t =
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

let get_record_by_path t path : record option Lwt.t =
  Util.use_pool t.db @@ Queries.get_record_by_path ~path
  >|= Option.map (fun (cid, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )

let get_record_by_cid t cid : record option Lwt.t =
  Util.use_pool t.db @@ Queries.get_record_by_cid ~cid
  >|= Option.map (fun (path, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )

let list_records t ?(limit = 100) ?(cursor = "") ?(reverse = false) collection :
    record list Lwt.t =
  let fn =
    if reverse then Queries.list_records_reverse else Queries.list_records
  in
  Util.use_pool t.db @@ fn ~collection ~limit ~cursor
  >|= List.map (fun (path, cid, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )

let put_record t record path : (Cid.t * bytes) Lwt.t =
  let cid, data = Lex.to_cbor_block record in
  let since = Tid.now () in
  let%lwt () =
    Util.use_pool t.db @@ Queries.put_record ~path ~cid ~data ~since
  in
  Lwt.return (cid, data)

(* blobs *)

let get_blob t cid : blob_with_contents option Lwt.t =
  match%lwt Util.use_pool t.db @@ Queries.get_blob ~cid with
  | None ->
      Lwt.return_none
  | Some blob ->
      let {id; cid; mimetype} : blob = blob in
      let file =
        Filename.concat
          (Util.Constants.user_blobs_location t.did)
          (Cid.to_string cid)
      in
      let data =
        In_channel.with_open_bin file In_channel.input_all |> Bytes.of_string
      in
      Lwt.return_some {id; cid; mimetype; data}

let list_blobs ?since t ~limit ~cursor : Cid.t list Lwt.t =
  Util.use_pool t.db
  @@
  match since with
  | Some since ->
      Queries.list_blobs_since ~limit ~cursor ~since
  | None ->
      Queries.list_blobs ~limit ~cursor

let put_blob t cid mimetype data : int Lwt.t =
  let file =
    Filename.concat
      (Util.Constants.user_blobs_location t.did)
      (Cid.to_string cid)
  in
  let () = Util.mkfile_p file ~perm:0o644 in
  let _ = Out_channel.with_open_bin file Out_channel.output_bytes data in
  Util.use_pool t.db @@ Queries.put_blob cid mimetype

let list_blob_refs t path : Cid.t list Lwt.t =
  Util.use_pool t.db @@ Queries.list_blob_refs path

let put_blob_ref t path cid : unit Lwt.t =
  Util.use_pool t.db @@ Queries.put_blob_ref path cid

let put_blob_refs t path cids : (unit, exn) Lwt_result.t =
  Lwt_result.map (fun _ -> ())
  @@ Util.multi_query t.db
       (List.map (fun cid -> Queries.put_blob_ref cid path) cids)

let clear_blob_refs t path cids : unit Lwt.t =
  Util.use_pool t.db @@ Queries.clear_blob_refs path cids
