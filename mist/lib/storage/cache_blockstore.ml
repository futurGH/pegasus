type 'bs data = {mutable reads: Cid.Set.t; bs: 'bs}

module Make
    (Bs : Blockstore.Writable) : sig
  include Blockstore.Writable

  val create : Bs.t -> t
end
with type t = Bs.t data = struct
  type t = Bs.t data

  let create bs = {reads= Cid.Set.empty; bs}

  let get_bytes t cid =
    match%lwt Bs.get_bytes t.bs cid with
    | Some _ as res ->
        t.reads <- Cid.Set.add cid t.reads ;
        Lwt.return res
    | None ->
        Lwt.return_none

  let has t cid = Bs.has t.bs cid

  let get_blocks t cids =
    let%lwt bm = Bs.get_blocks t.bs cids in
    t.reads <-
      Cid.Set.union t.reads (Cid.Set.of_list (Block_map.keys bm.blocks)) ;
    Lwt.return bm

  let put_block t cid bytes = Bs.put_block t.bs cid bytes

  let put_many t blocks = Bs.put_many t.bs blocks

  let delete_block t cid = Bs.delete_block t.bs cid

  let delete_many t cids = Bs.delete_many t.bs cids
end
