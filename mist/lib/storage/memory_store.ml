module Make () = struct
  type t =
    { mutable blocks: Block_map.t
    ; mutable root: Cid.t option
    ; mutable rev: string option }

  let create ?(blocks = Block_map.empty) () = {blocks; root= None; rev= None}

  let get_bytes s cid = Lwt.return (Block_map.get cid s.blocks)

  let has s cid = Lwt.return (Block_map.has cid s.blocks)

  let get_blocks s cids = Lwt.return (Block_map.get_many cids s.blocks)

  let put_block s cid bytes ~rev =
    s.blocks <- Block_map.set cid bytes s.blocks ;
    s.rev <- Some rev ;
    Lwt.return_unit

  let put_many s blocks =
    s.blocks <- Block_map.merge s.blocks blocks ;
    Lwt.return_unit

  let update_root s cid ~rev =
    s.root <- Some cid ;
    s.rev <- Some rev ;
    Lwt.return_unit

  let apply_commit s (c : Repo_store.commit_data) =
    let with_removed =
      Cid.Set.fold
        (fun cid blocks -> Block_map.remove cid blocks)
        c.removed_cids s.blocks
    in
    s.blocks <- Block_map.merge with_removed c.relevant_blocks ;
    s.root <- Some c.cid ;
    Lwt.return_unit
end
