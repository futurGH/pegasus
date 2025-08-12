module Block_map = Mist.Storage.Block_map

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

let ( let*! ) m f =
  match%lwt m with Ok x -> f x | Error e -> failwith (Caqti_error.show e)

module Queries = struct
  let create_table =
    [%rapper
      execute
        {sql| CREATE TABLE IF NOT EXISTS blocks (
                cid TEXT NOT NULL PRIMARY KEY,
                data BLOB NOT NULL
              );
        |sql}]
      ()

  let get_block cid =
    [%rapper
      get_opt
        {sql| SELECT @Cid{cid}, @Blob{data} FROM blocks WHERE cid = %Cid{cid} |sql}
        record_out]
      ~cid

  let get_blocks cids =
    [%rapper
      get_many
        {sql| SELECT @Cid{cid}, @Blob{data} FROM blocks WHERE cid IN (%list{%Cid{cids}}) |sql}
        record_out]
      ~cids

  let has_block cid =
    [%rapper
      get_opt {sql| SELECT @Cid{cid} FROM blocks WHERE cid = %Cid{cid} |sql}]
      ~cid

  let put_block cid block =
    [%rapper
      execute
        {sql| INSERT INTO blocks (cid, data) VALUES (%Cid{cid}, %Blob{block}) |sql}]
      ~cid ~block

  let delete_block cid =
    [%rapper execute {sql| DELETE FROM blocks WHERE cid = %Cid{cid} |sql}] ~cid

  let delete_blocks cids =
    [%rapper
      execute {sql| DELETE FROM blocks WHERE cid IN (%list{%Cid{cids}}) |sql}]
      ~cids
end

module S (C : Caqti_lwt.CONNECTION) : Mist.Storage.Writable_blockstore = struct
  include Caqti_type.Std
  include Caqti_request.Infix
  include Lwt_result.Syntax

  type t = {connection: Caqti_lwt.connection}

  let multi_query queries =
    let*! () = C.start () in
    match%lwt
      Lwt_list.fold_left_s
        (fun acc query -> Lwt_result.bind (query ()) (fun () -> Lwt.return acc))
        (Ok ()) queries
    with
    | Ok () ->
        C.commit ()
    | Error e ->
        let*! () = C.rollback () in
        Lwt.return_error e

  let init t =
    let*! () =
      [%rapper execute {sql| PRAGMA journal_mode=WAL; |sql} syntax_off]
        () t.connection
    in
    let*! () =
      [%rapper execute {sql| PRAGMA synchronous=NORMAL; |sql} syntax_off]
        () t.connection
    in
    let*! () = Queries.create_table t.connection in
    Lwt.return_unit

  let get_bytes t cid =
    let*! b_opt = Queries.get_block cid t.connection in
    match b_opt with
    | Some {data; _} ->
        Lwt.return_some data
    | None ->
        Lwt.return_none

  let get_blocks t cids =
    let*! blocks = Queries.get_blocks cids t.connection in
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
    let*! b_opt = Queries.has_block cid t.connection in
    match b_opt with Some _ -> Lwt.return true | None -> Lwt.return false

  let put_block t cid block =
    let*! () = Queries.put_block cid block t.connection in
    Lwt.return_unit

  let put_many t bm =
    let*! () =
      multi_query
        (List.map
           (fun (cid, block) ->
             fun () -> Queries.put_block cid block t.connection )
           (Block_map.entries bm) )
    in
    Lwt.return_unit

  let delete_block t cid =
    let*! () = Queries.delete_block cid t.connection in
    Lwt.return_unit

  let delete_many t cids =
    let*! () = Queries.delete_blocks cids t.connection in
    Lwt.return_unit
end

let connect db_uri =
  match%lwt Caqti_lwt.connect (Uri.of_string db_uri) with
  | Ok c ->
      let module C = (val c : Caqti_lwt.CONNECTION) in
      let module Store = S (C) in
      Lwt.return (module Store : Mist.Storage.Writable_blockstore)
  | Error e ->
      failwith (Caqti_error.show e)
