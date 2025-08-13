module Make () = struct
  type t = {mutable blocks: Block_map.t}

  let create ?(blocks = Block_map.empty) () = {blocks}

  let get_bytes s cid = Lwt.return (Block_map.get cid s.blocks)

  let has s cid = Lwt.return (Block_map.has cid s.blocks)

  let get_blocks s cids = Lwt.return (Block_map.get_many cids s.blocks)

  let put_block s cid bytes =
    s.blocks <- Block_map.set cid bytes s.blocks ;
    Lwt.return_ok true

  let put_many s blocks =
    s.blocks <- Block_map.merge s.blocks blocks ;
    Lwt.return_ok (Block_map.size blocks)

  let delete_block s cid =
    s.blocks <- Block_map.remove cid s.blocks ;
    Lwt.return_ok true

  let delete_many s cids =
    s.blocks <-
      List.fold_left
        (fun blocks cid -> Block_map.remove cid blocks)
        s.blocks cids ;
    Lwt.return_ok (List.length cids)
end
