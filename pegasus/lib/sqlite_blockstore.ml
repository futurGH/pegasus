open Util.Syntax
module Block_map = Mist.Storage.Block_map

let caqti_result_exn = function
  | Ok x ->
      Ok x
  | Error caqti_err ->
      Error (Caqti_error.Exn caqti_err)

module Cid : Rapper.CUSTOM with type t = Cid.t = struct
  type t = Cid.t

  let t =
    let encode cid =
      try Ok (Cid.to_string cid) with e -> Error (Printexc.to_string e)
    in
    Caqti_type.(custom ~encode ~decode:Cid.of_string string)
end

module Blob : Rapper.CUSTOM with type t = bytes = struct
  type t = bytes

  let t =
    let encode blob =
      try Ok (Bytes.to_string blob) with e -> Error (Printexc.to_string e)
    in
    let decode blob =
      try Ok (Bytes.of_string blob) with e -> Error (Printexc.to_string e)
    in
    Caqti_type.(custom ~encode ~decode string)
end

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
        {sql| INSERT INTO blocks (cid, data) VALUES (%Cid{cid}, %Blob{block}) ON CONFLICT DO NOTHING RETURNING @Cid{cid} |sql}]
      ~cid ~block

  let delete_block cid =
    [%rapper execute {sql| DELETE FROM blocks WHERE cid = %Cid{cid} |sql}] ~cid

  let delete_blocks cids =
    [%rapper
      get_many
        {sql| DELETE FROM blocks WHERE cid IN (%list{%Cid{cids}}) RETURNING @Cid{cid} |sql}]
      ~cids
end

module S (C : Caqti_lwt.CONNECTION) : Mist.Storage.Writable_blockstore = struct
  include Caqti_type.Std
  include Caqti_request.Infix
  include Lwt_result.Syntax

  type t = {connection: Caqti_lwt.connection}

  let multi_query (queries : (unit -> ('a, Caqti_error.t) Lwt_result.t) list) =
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
    let%lwt results =
      Lwt_list.map_s
        (fun query ->
          match%lwt query () with
          | Ok _ ->
              Lwt.return true
          | Error e ->
              if is_ignorable_error e then Lwt.return false
              else failwith (Caqti_error.show e) )
        queries
    in
    let inserted =
      List.fold_left
        (fun acc success -> if success then acc + 1 else acc)
        0 results
    in
    Lwt.return_ok inserted

  let init t =
    let$! () =
      [%rapper execute {sql| PRAGMA journal_mode=WAL; |sql} syntax_off]
        () t.connection
    in
    let$! () =
      [%rapper execute {sql| PRAGMA synchronous=NORMAL; |sql} syntax_off]
        () t.connection
    in
    let$! () = Queries.create_table t.connection in
    Lwt.return_unit

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
      multi_query
        (List.map
           (fun (cid, block) ->
             fun () -> Queries.put_block cid block t.connection )
           (Block_map.entries bm) )
    in
    Lwt.return_ok inserted

  let delete_block t cid =
    let$! () = Queries.delete_block cid t.connection in
    Lwt.return_ok true

  let delete_many t cids =
    let$! deleted = Queries.delete_blocks cids t.connection in
    Lwt.return_ok (List.length deleted)
end

let connect db_uri =
  match%lwt Caqti_lwt.connect (Uri.of_string db_uri) with
  | Ok c ->
      let module C = (val c : Caqti_lwt.CONNECTION) in
      let module Store = S (C) in
      Lwt.return (module Store : Mist.Storage.Writable_blockstore)
  | Error e ->
      failwith (Caqti_error.show e)
