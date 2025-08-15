open Util.Rapper
open Util.Syntax
module Block_map = Mist.Storage.Block_map

type t = {connection: Caqti_lwt.connection}

type block = {cid: Cid.t; data: Blob.t}

module Queries = struct
  let create_table =
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
        {sql| SELECT @Cid{cid}, @Blob{data} FROM mst WHERE cid = %Cid{cid} |sql}
        record_out]
      ~cid

  let get_blocks cids =
    [%rapper
      get_many
        {sql| SELECT @Cid{cid}, @Blob{data} FROM mst WHERE cid IN (%list{%Cid{cids}}) |sql}
        record_out]
      ~cids

  let has_block cid =
    [%rapper
      get_opt {sql| SELECT @Cid{cid} FROM mst WHERE cid = %Cid{cid} |sql}]
      ~cid

  let put_block cid block =
    [%rapper
      get_opt
        {sql| INSERT INTO mst (cid, data) VALUES (%Cid{cid}, %Blob{block}) ON CONFLICT DO NOTHING RETURNING @Cid{cid} |sql}]
      ~cid ~block

  let delete_block cid =
    [%rapper execute {sql| DELETE FROM mst WHERE cid = %Cid{cid} |sql}] ~cid

  let delete_blocks cids =
    [%rapper
      get_many
        {sql| DELETE FROM mst WHERE cid IN (%list{%Cid{cids}}) RETURNING @Cid{cid} |sql}]
      ~cids
end

let caqti_result_exn = function
  | Ok x ->
      Ok x
  | Error caqti_err ->
      Error (Caqti_error.Exn caqti_err)

let multi_query connection
    (queries : (unit -> ('a, Caqti_error.t) Lwt_result.t) list) =
  let module C = (val connection : Caqti_lwt.CONNECTION) in
  let$! () = C.start () in
  let is_ignorable_error e =
    match (e : Caqti_error.t) with
    | `Request_failed qe | `Response_failed qe -> (
      match Caqti_error.cause (`Request_failed qe) with
      | `Not_null_violation | `Unique_violation ->
          true
      | _ ->
          false )
    | _ ->
        false
  in
  let rec aux acc queries =
    match acc with
    | Error e ->
        Lwt.return_error e
    | Ok count -> (
      match queries with
      | [] ->
          Lwt.return (Ok count)
      | query :: rest -> (
          let%lwt result = query () in
          match result with
          | Ok _ ->
              aux (Ok (count + 1)) rest
          | Error e ->
              if is_ignorable_error e then aux (Ok count) rest
              else Lwt.return_error e ) )
  in
  aux (Ok 0) queries

let connect db_uri =
  let%lwt connection = Util.connect_sqlite db_uri in
  let$! () = Queries.create_table connection in
  Lwt.return {connection}

let get_bytes t cid =
  let$! b_opt = Queries.get_block cid t.connection in
  match b_opt with
  | Some {data; _} ->
      Lwt.return_some data
  | None ->
      Lwt.return_none

let get_blocks t cids =
  let$! blocks = Queries.get_blocks cids t.connection in
  Lwt.return
    (List.fold_left
       (fun (acc : Block_map.with_missing) cid ->
         match List.find_opt (fun b -> b.cid = cid) blocks with
         | Some {data; _} ->
             {acc with blocks= Block_map.set cid data acc.blocks}
         | None ->
             {acc with missing= cid :: acc.missing} )
       {blocks= Block_map.empty; missing= []}
       cids )

let has t cid =
  let$! b_opt = Queries.has_block cid t.connection in
  match b_opt with Some _ -> Lwt.return true | None -> Lwt.return false

let put_block t cid block =
  Queries.put_block cid block t.connection
  |> Lwt.map caqti_result_exn
  |> Lwt.map (Result.map (function Some _ -> true | None -> false))

let put_many t bm =
  let$! inserted =
    multi_query t.connection
      (List.map
         (fun (cid, block) -> fun () -> Queries.put_block cid block t.connection)
         (Block_map.entries bm) )
  in
  Lwt.return_ok inserted

let delete_block t cid =
  let$! () = Queries.delete_block cid t.connection in
  Lwt.return_ok true

let delete_many t cids =
  let$! deleted = Queries.delete_blocks cids t.connection in
  Lwt.return_ok (List.length deleted)
