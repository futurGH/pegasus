module Make (Top : Blockstore.Readable) (Bottom : Blockstore.Readable) : sig
  include Blockstore.Readable

  val create : Top.t -> Bottom.t -> t
end = struct
  type t = {top: Top.t; bottom: Bottom.t}

  let create top bottom = {top; bottom}

  let get_bytes {top; bottom} cid =
    match%lwt Top.get_bytes top cid with
    | Some _ as res ->
        Lwt.return res
    | None ->
        Bottom.get_bytes bottom cid

  let has {top; bottom} cid =
    match%lwt Top.has top cid with
    | true ->
        Lwt.return_true
    | false ->
        Bottom.has bottom cid

  let get_blocks {top; bottom} cids =
    let%lwt from_top = Top.get_blocks top cids in
    let%lwt from_bottom = Bottom.get_blocks bottom from_top.missing in
    let merged_blocks = Block_map.merge from_top.blocks from_bottom.blocks in
    Lwt.return {Block_map.blocks= merged_blocks; missing= from_bottom.missing}
end
