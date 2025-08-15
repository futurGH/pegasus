open Util.Rapper
open Util.Syntax
module Lex = Mist.Lex
module Tid = Mist.Tid

type t = Caqti_lwt.connection

type blob = {id: int; cid: Cid.t; mimetype: string; data: Blob.t}

module Queries = struct
  let create_tables t =
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
        () t
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
        () t
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
      () t

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

let init connection = Queries.create_tables connection

let get_blob t cid : blob option Lwt.t =
  let$! blob = Queries.get_blob t ~cid in
  Lwt.return blob

let list_blobs t ~limit ~cursor : Cid.t list Lwt.t =
  let$! blobs = Queries.list_blobs t ~limit ~cursor in
  Lwt.return blobs

let write_blob t cid mimetype data : int Lwt.t =
  let$! blob_id = Queries.write_blob t cid mimetype data in
  Lwt.return blob_id
