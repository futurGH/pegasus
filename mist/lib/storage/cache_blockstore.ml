type 'bs data = {mutable reads: Cid.Set.t; mutable cache: Block_map.t; bs: 'bs}

module Make
    (Bs : Blockstore.Writable) : sig
  include Blockstore.Writable

  val create : Bs.t -> t

  val get_reads : t -> Cid.Set.t

  val get_cache : t -> Block_map.t
end
with type t = Bs.t data = struct
  type t = Bs.t data

  let create bs = {reads= Cid.Set.empty; cache= Block_map.empty; bs}

  let get_reads t = t.reads

  let get_cache t = t.cache

  let get_bytes t cid =
    match Block_map.get cid t.cache with
    | Some _ as cached ->
        t.reads <- Cid.Set.add cid t.reads ;
        Lwt.return cached
    | None -> (
      match%lwt Bs.get_bytes t.bs cid with
      | Some data as res ->
          t.cache <- Block_map.set cid data t.cache ;
          t.reads <- Cid.Set.add cid t.reads ;
          Lwt.return res
      | None ->
          Lwt.return_none )

  let has t cid =
    if Block_map.has cid t.cache then Lwt.return_true else Bs.has t.bs cid

  let get_blocks t cids =
    let {Block_map.blocks= cached; missing} = Block_map.get_many cids t.cache in
    (* mark cached as read *)
    Block_map.iter (fun cid _ -> t.reads <- Cid.Set.add cid t.reads) cached ;
    (* fetch missing from underlying store *)
    let%lwt fetched = Bs.get_blocks t.bs missing in
    (* cache and mark as read *)
    Block_map.iter
      (fun cid data ->
        t.cache <- Block_map.set cid data t.cache ;
        t.reads <- Cid.Set.add cid t.reads )
      fetched.blocks ;
    (* combine results *)
    let blocks =
      List.fold_left
        (fun acc (cid, data) -> Block_map.set cid data acc)
        fetched.blocks (Block_map.entries cached)
    in
    Lwt.return {Block_map.blocks; missing= fetched.missing}

  let put_block t cid bytes =
    t.cache <- Block_map.set cid bytes t.cache ;
    Bs.put_block t.bs cid bytes

  let put_many t blocks =
    Block_map.iter
      (fun cid data -> t.cache <- Block_map.set cid data t.cache)
      blocks ;
    Bs.put_many t.bs blocks

  let delete_block t cid =
    t.cache <- Block_map.remove cid t.cache ;
    Bs.delete_block t.bs cid

  let delete_many t cids =
    List.iter (fun cid -> t.cache <- Block_map.remove cid t.cache) cids ;
    Bs.delete_many t.bs cids
end
