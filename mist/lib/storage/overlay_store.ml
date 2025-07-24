let ( let* ) = Lwt.bind

module Make (Top : Repo_store.Readable) (Bottom : Repo_store.Readable) : sig
  include Repo_store.Readable

  val create : Top.t -> Bottom.t -> t
end = struct
  type t = {top: Top.t; bottom: Bottom.t}

  let create top bottom = {top; bottom}

  let get_bytes {top; bottom} cid =
    let* from_top = Top.get_bytes top cid in
    match from_top with
    | Some _ as res ->
        Lwt.return res
    | None ->
        Bottom.get_bytes bottom cid

  let has {top; bottom} cid =
    let* from_top = Top.has top cid in
    match from_top with
    | true ->
        Lwt.return_true
    | false ->
        Bottom.has bottom cid

  let get_blocks {top; bottom} cids =
    let* from_top = Top.get_blocks top cids in
    let* from_bottom = Bottom.get_blocks bottom from_top.missing in
    let merged_blocks = Block_map.merge from_top.blocks from_bottom.blocks in
    Lwt.return {Block_map.blocks= merged_blocks; missing= from_bottom.missing}

  let read_obj_and_bytes {top; bottom} cid =
    let* from_top = Top.read_obj_and_bytes top cid in
    match from_top with
    | Some _ as res ->
        Lwt.return res
    | None ->
        Bottom.read_obj_and_bytes bottom cid

  let read_obj {top; bottom} cid =
    let* from_top = Top.read_obj top cid in
    match from_top with
    | Some _ as res ->
        Lwt.return res
    | None ->
        Bottom.read_obj bottom cid

  let read_record {top; bottom} cid =
    Lwt.catch
      (fun () -> Top.read_record top cid)
      (fun _ -> Bottom.read_record bottom cid)
end
