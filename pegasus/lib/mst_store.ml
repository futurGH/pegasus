open Util.Rapper
open Util.Syntax
module Block_map = Mist.Storage.Block_map

type t = Caqti_lwt.connection

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
end

let init connection = Queries.create_table connection

let get_bytes t cid =
  Queries.get_block cid t
  >$! function
  | Some {data; _} ->
      Lwt.return_some data
  | None ->
      Lwt.return_none

let get_blocks t cids =
  let$! blocks = Queries.get_blocks cids t in
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
  Queries.has_block cid t
  >$! function Some _ -> Lwt.return true | None -> Lwt.return false

let put_block t cid block =
  Queries.put_block cid block t
  |> Lwt.map Util.caqti_result_exn
  |> Lwt.map (Result.map (function Some _ -> true | None -> false))

let put_many t bm =
  Util.multi_query t
    (List.map
       (fun (cid, block) -> fun () -> Queries.put_block cid block t)
       (Block_map.entries bm) )
  >$! Lwt.return_ok

let delete_block t cid =
  let$! () = Queries.delete_block cid t in
  Lwt.return_ok true

let delete_many t cids =
  Queries.delete_blocks cids t >$! List.length |> Lwt.return_ok
