open Lwt.Infix
open Util.Rapper
open Util.Syntax
module Block_map = Mist.Storage.Block_map
module Lex = Mist.Lex
module Tid = Mist.Tid

type block = {cid: Cid.t; data: Blob.t}

type record = {path: string; cid: Cid.t; value: Lex.repo_record; since: Tid.t}

type blob = {id: int; cid: Cid.t; mimetype: string; data: Blob.t}

module Queries = struct
  (* mst storage *)
  let create_mst_table =
    [%rapper
      execute
        {sql| CREATE TABLE IF NOT EXISTS mst (
                cid TEXT NOT NULL PRIMARY KEY,
                data BLOB NOT NULL
              );
        |sql}]
      ()

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

  (* record storage *)
  let create_records_table =
    [%rapper
      execute
        {sql| CREATE TABLE IF NOT EXISTS records (
                path TEXT NOT NULL PRIMARY KEY,
                cid TEXT NOT NULL,
                data BLOB NOT NULL,
                since TEXT NOT NULL
              );
        |sql}]
      ()

  let get_record_by_path =
    [%rapper
      get_opt
        {sql| SELECT @CID{cid}, @Blob{data}, @string{since} FROM records WHERE path = %string{path}
      |sql}]

  let get_record_by_cid =
    [%rapper
      get_opt
        {sql| SELECT @string{path}, @Blob{data}, @string{since} FROM records WHERE cid = %CID{cid}
      |sql}]

  let list_records =
    [%rapper
      get_many
        {sql| SELECT @string{path}, @CID{cid}, @Blob{data}, @string{since} FROM records WHERE path LIKE %string{collection}/%
      |sql}]

  let write_record =
    [%rapper
      execute
        {sql| INSERT INTO records (path, cid, data, since)
                VALUES (%string{path}, %CID{cid}, %Blob{data}, %string{since})
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

  let write_blob cid mimetype data =
    [%rapper
      get_one
        {sql| INSERT INTO blobs (cid, mimetype, data) VALUES (%CID{cid}, %string{mimetype}, %Blob{data}) RETURNING @int{id} |sql}]
      ~cid ~mimetype ~data
end

let init conn : unit Lwt.t =
  let$! () = Queries.create_mst_table conn in
  let$! () = Queries.create_records_table conn in
  let$! () = Queries.create_blobs_tables conn in
  Lwt.return_unit

(* mst *)

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

let list_records conn collection : record list Lwt.t =
  Queries.list_records ~collection conn
  >$! List.map (fun (path, cid, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )
  >>= Lwt.return

let write_record conn record path : unit Lwt.t =
  let cid, data = Lex.to_cbor_block record in
  let since = Tid.now () in
  let$! () = Queries.write_record ~path ~cid ~data ~since conn in
  Lwt.return_unit

(* blobs *)

let get_blob conn cid : blob option Lwt.t =
  let$! blob = Queries.get_blob conn ~cid in
  Lwt.return blob

let list_blobs conn ~limit ~cursor : Cid.t list Lwt.t =
  let$! blobs = Queries.list_blobs conn ~limit ~cursor in
  Lwt.return blobs

let write_blob conn cid mimetype data : int Lwt.t =
  let$! blob_id = Queries.write_blob conn cid mimetype data in
  Lwt.return blob_id
