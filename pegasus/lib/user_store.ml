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

  type blob = {id: int; cid: Cid.t; mimetype: string; data: Blob.t}
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
                );
          |sql}]
        () conn
    in
    [%rapper
      execute
        {sql| CREATE TABLE IF NOT EXISTS commit (
                id INTEGER PRIMARY KEY CHECK (id = 0),
                cid TEXT NOT NULL,
                data BLOB NOT NULL
              );
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
            FROM commit WHERE id = 0
      |sql}]
      ()

  let put_commit cid data =
    [%rapper
      execute
        {sql| INSERT INTO commit (id, cid, data)
            VALUES (0, %CID{cid}, %Blob{data})
            ON CONFLICT(id) DO UPDATE SET
              cid = excluded.cid,
              data = excluded.data
      |sql}]
      ~cid ~data

  (* record storage *)
  let create_records_table =
    [%rapper
      execute
        {sql| CREATE TABLE IF NOT EXISTS records (
                path TEXT NOT NULL PRIMARY KEY,
                cid TEXT NOT NULL,
                since TEXT NOT NULL,
                data BLOB NOT NULL
              );
              CREATE INDEX IF NOT EXISTS records_cid_idx ON records (cid);
        |sql}]
      ()

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
              WHERE path LIKE %string{collection}/%
              ORDER BY since DESC LIMIT %int{limit} OFFSET %int{offset}
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
                mimetype TEXT NOT NULL,
                data BLOB NOT NULL
              );
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
                );
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
                END;
        |sql}
        syntax_off]
      () conn

  let get_blob =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @CID{cid}, @string{mimetype}, @Blob{data} FROM blobs WHERE cid = %CID{cid} |sql}
        record_out]

  let list_blobs ~limit ~cursor =
    [%rapper
      get_many
        {sql| SELECT @CID{cid} FROM blobs WHERE id > %int{cursor} ORDER BY id LIMIT %int{limit} |sql}]
      ~limit ~cursor

  let put_blob cid mimetype data =
    [%rapper
      get_one
        {sql| INSERT INTO blobs (cid, mimetype, data)
              VALUES (%CID{cid}, %string{mimetype}, %Blob{data})
              ON CONFLICT (cid) DO UPDATE SET mimetype = excluded.mimetype, data = excluded.data
              RETURNING @int{id}
        |sql}]
      ~cid ~mimetype ~data

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
        {sql| DELETE FROM blobs_records WHERE record_path LIKE %string{path} AND blob_id IN (
                    SELECT id FROM blobs WHERE cid IN (%list{%CID{cids}})
                  )
        |sql}]
      ~path ~cids
end

type t = (module Rapper_helper.CONNECTION)

let init conn : unit Lwt.t =
  let$! () = Queries.create_blocks_tables conn in
  let$! () = Queries.create_records_table conn in
  let$! () = Queries.create_blobs_tables conn in
  Lwt.return_unit

(* mst blocks; implements Writable_blockstore *)

let get_bytes conn cid : Blob.t option Lwt.t =
  Queries.get_block cid conn
  >$! function Some {data; _} -> Some data | None -> None

let get_blocks conn cids : Block_map.with_missing Lwt.t =
  let$! blocks = Queries.get_blocks cids conn in
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

let has conn cid : bool Lwt.t =
  Queries.has_block cid conn >$! function Some _ -> true | None -> false

let put_block conn cid block : (bool, exn) Lwt_result.t =
  Queries.put_block cid block conn
  |> Lwt.map Util.caqti_result_exn
  |> Lwt.map (Result.map (function Some _ -> true | None -> false))

let put_many conn bm : (int, exn) Lwt_result.t =
  Util.multi_query conn
    (List.map
       (fun (cid, block) -> fun () -> Queries.put_block cid block conn)
       (Block_map.entries bm) )

let delete_block conn cid : (bool, exn) Lwt_result.t =
  let$! () = Queries.delete_block cid conn in
  Lwt.return_ok true

let delete_many conn cids : (int, exn) Lwt_result.t =
  Queries.delete_blocks cids conn >$! List.length >>= Lwt.return_ok

let clear_mst conn : unit Lwt.t =
  let$! () = Queries.clear_mst conn in
  Lwt.return_unit

(* repo commit *)

let get_commit conn : (Cid.t * signed_commit) option Lwt.t =
  Queries.get_commit conn
  >$! Option.map (fun (cid, data) ->
          ( cid
          , data |> Dag_cbor.decode_to_yojson |> signed_commit_of_yojson
            |> Result.get_ok ) )

let put_commit conn commit : (Cid.t, exn) Lwt_result.t =
  let data = commit |> signed_commit_to_yojson |> Dag_cbor.encode_yojson in
  let cid = Cid.create Dcbor data in
  let$! () = Queries.put_commit cid data conn in
  Lwt.return_ok cid

(* records *)

let get_record_by_path conn path : record option Lwt.t =
  Queries.get_record_by_path ~path conn
  >$! Option.map (fun (cid, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )
  >>= Lwt.return

let get_record_by_cid conn cid : record option Lwt.t =
  Queries.get_record_by_cid ~cid conn
  >$! Option.map (fun (path, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )
  >>= Lwt.return

let list_records conn ?(limit = 100) ?(offset = 0) collection :
    record list Lwt.t =
  Queries.list_records ~collection ~limit ~offset conn
  >$! List.map (fun (path, cid, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )
  >>= Lwt.return

let put_record conn record path : Cid.t Lwt.t =
  let cid, data = Lex.to_cbor_block record in
  let since = Tid.now () in
  let$! () = Queries.put_record ~path ~cid ~data ~since conn in
  Lwt.return cid

(* blobs *)

let get_blob conn cid : blob option Lwt.t = unwrap @@ Queries.get_blob conn ~cid

let list_blobs conn ~limit ~cursor : Cid.t list Lwt.t =
  unwrap @@ Queries.list_blobs conn ~limit ~cursor

let put_blob conn cid mimetype data : int Lwt.t =
  unwrap @@ Queries.put_blob cid mimetype data conn

let list_blob_refs conn path : Cid.t list Lwt.t =
  unwrap @@ Queries.list_blob_refs path conn

let put_blob_ref conn path cid : unit Lwt.t =
  unwrap @@ Queries.put_blob_ref path cid conn

let put_blob_refs conn path cids : (unit, exn) Lwt_result.t =
  Lwt_result.map (fun _ -> ())
  @@ Util.multi_query conn
       (List.map
          (fun cid -> fun () -> Queries.put_blob_ref cid path conn)
          cids )

let clear_blob_refs conn path cids : unit Lwt.t =
  unwrap @@ Queries.clear_blob_refs path cids conn
