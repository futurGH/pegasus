open Lwt.Infix
open Util.Rapper
open Util.Syntax
module Lex = Mist.Lex
module Tid = Mist.Tid

type t = Caqti_lwt.connection

type record = {path: string; cid: Cid.t; value: Lex.repo_record; since: Tid.t}

module Queries = struct
  let create_table =
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
end

let init connection = Queries.create_table connection

let get_record_by_path t path : record option Lwt.t =
  Queries.get_record_by_path ~path t
  >$! Option.map (fun (cid, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )
  >>= Lwt.return

let get_record_by_cid t cid : record option Lwt.t =
  Queries.get_record_by_cid ~cid t
  >$! Option.map (fun (path, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )
  >>= Lwt.return

let list_records t collection : record list Lwt.t =
  Queries.list_records ~collection t
  >$! List.map (fun (path, cid, data, since) ->
          {path; cid; value= Lex.of_cbor data; since} )
  >>= Lwt.return

let write_record t record path : unit Lwt.t =
  let cid, data = Lex.to_cbor_block record in
  let since = Tid.now () in
  let$! () = Queries.write_record ~path ~cid ~data ~since t in
  Lwt.return_unit
