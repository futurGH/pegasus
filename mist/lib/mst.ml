module StringMap = Dag_cbor.StringMap

type node_raw =
  { (* link to lower level left subtree with all keys sorting before this node *)
    l: Cid.t option
  ; (* ordered list of entries in this node *)
    e: entry_raw list }

and entry_raw =
  { (* length of prefix shared with previous path *)
    p: int
  ; (* remainder of path for this entry, after the first p characters *)
    k: bytes
  ; (* link to the CBOR record data for this entry *)
    v: Cid.t
  ; (* link to lower level right subtree with all keys sorting after this entry, but before the next entry *)
    t: Cid.t option }

let encode_entry_raw entry : Dag_cbor.value =
  `Map
    (StringMap.of_list
       [ ("p", `Integer (Int64.of_int entry.p))
       ; ("k", `Bytes entry.k)
       ; ("v", `Link entry.v)
       ; ("t", match entry.t with Some t -> `Link t | None -> `Null) ] )

let encode_node_raw node : Dag_cbor.value =
  `Map
    (StringMap.of_list
       [ ("l", match node.l with Some l -> `Link l | None -> `Null)
       ; ("e", `Array (Array.of_list (List.map encode_entry_raw node.e))) ] )

type node =
  { layer: int
  ; mutable left: node option Lwt.t Lazy.t
  ; mutable entries: entry list }

and entry =
  {layer: int; key: string; value: Cid.t; right: node option Lwt.t Lazy.t}

let ( let*? ) lazy_opt_lwt f =
  let%lwt result = Lazy.force lazy_opt_lwt in
  f result

let ( >>? ) lazy_opt_lwt f =
  let%lwt result = Lazy.force lazy_opt_lwt in
  f result

(* figures out where to put an entry in or below a hydrated node, returns new node *)
let rec insert_entry node entry : node Lwt.t =
  let entry_layer = Util.leading_zeros_on_hash entry.key in
  (* as long as node layer <= entry layer, create a new node above node
     until we have a node at the correct height for the entry to be inserted *)
  let rec build_insert_node node layer =
    if layer >= entry_layer then node
    else
      build_insert_node
        {layer= layer + 1; left= lazy (Lwt.return_some node); entries= []}
        (layer + 1)
  in
  let insert_node = build_insert_node node node.layer in
  (* if entry is below node, recursively insert into node's left subtree *)
  if entry_layer < insert_node.layer then
    let*? left = insert_node.left in
    match (insert_node.entries, left) with
    | [], None ->
        failwith "found totally empty mst node"
    | [], Some left ->
        let%lwt left_inserted = insert_entry left entry in
        node.left <- lazy (Lwt.return_some left_inserted) ;
        Lwt.return insert_node
    | _ ->
        Lwt.return insert_node
  else (
    (* if entry is at this node's layer, append it to node's entries,
       checking that its key occurs after the last existing entry *)
    assert (node.layer = entry_layer) ;
    ( match Util.last node.entries with
    | Some last ->
        (* we can assert this because hydrate_from_map calls this function
           while iterating over keys in sorted order *)
        assert (entry.key > last.key)
    | None ->
        () ) ;
    node.entries <- node.entries @ [entry] ;
    Lwt.return node )

(* helper to find the entry with a given key in a hydrated node *)
let find_entry_nonrec node key =
  let rec aux entries =
    match entries with
    | [] ->
        None
    | e :: es ->
        if e.key = key then Some e else if e.key > key then None else aux es
  in
  aux node.entries

(* from a list of raw entries, produces a list of their keys *)
let entries_to_keys entries =
  entries
  |> List.fold_left
       (fun keys entry ->
         let prefix =
           match keys with [] -> "" | prev :: _ -> String.sub prev 0 entry.p
         in
         let path = String.concat "" [prefix; Bytes.to_string entry.k] in
         Util.ensure_valid_key path ; path :: keys )
       []
  |> List.rev

module Make (Store : Storage.Writable_blockstore) = struct
  type bs = Store.t

  type t = {blockstore: bs; root: Cid.t}

  let create blockstore root = {blockstore; root}

  (* decodes a node retrieved from the blockstore *)
  let decode_block_raw b : node_raw =
    match Dag_cbor.decode b with
    | `Map node ->
        if not (StringMap.mem "e" node) then
          raise (Invalid_argument "mst node missing 'e'") ;
        let l =
          if StringMap.mem "l" node then
            match StringMap.find "l" node with `Link l -> Some l | _ -> None
          else None
        in
        let e_array =
          match StringMap.find "e" node with `Array e -> e | _ -> [||]
        in
        let e =
          Array.to_list
          @@ Array.map
               (fun (entry : Dag_cbor.value) ->
                 match entry with
                 | `Map entry ->
                     { p=
                         ( entry |> StringMap.find "p"
                         |> function
                         | `Integer p ->
                             Int64.to_int p
                         | _ ->
                             raise (Invalid_argument "mst entry missing 'p'") )
                     ; k=
                         ( entry |> StringMap.find "k"
                         |> function
                         | `Bytes k ->
                             k
                         | _ ->
                             raise (Invalid_argument "mst entry missing 'k'") )
                     ; v=
                         ( entry |> StringMap.find "v"
                         |> function
                         | `Link v ->
                             v
                         | _ ->
                             raise (Invalid_argument "mst entry missing 'v'") )
                     ; t=
                         ( entry |> StringMap.find "t"
                         |> function `Link t -> Some t | _ -> None ) }
                 | _ ->
                     raise (Invalid_argument "non-map mst entry") )
               e_array
        in
        {l; e}
    | _ ->
        raise (Invalid_argument "invalid block")

  let retrieve_node_raw t cid : node_raw option Lwt.t =
    match%lwt Store.get_bytes t.blockstore cid with
    | Some bytes ->
        bytes |> decode_block_raw |> Lwt.return_some
    | None ->
        Lwt.return_none

  (* retrieves & decodes a node by cid *)
  let rec retrieve_node t cid : node option Lwt.t =
    match%lwt retrieve_node_raw t cid with
    | Some raw ->
        hydrate_node t raw |> Lwt.map Option.some
    | None ->
        Lwt.return_none

  and retrieve_node_lazy t cid = lazy (retrieve_node t cid)

  (* hydrates a raw node *)
  and hydrate_node t node_raw : node Lwt.t =
    let left =
      match node_raw.l with
      | Some l ->
          retrieve_node_lazy t l
      | None ->
          lazy Lwt.return_none
    in
    let%lwt layer = get_node_height t node_raw in
    let entries =
      List.fold_left
        (fun entries entry ->
          let prefix =
            match entries with
            | [] ->
                ""
            | prev :: _ ->
                String.sub prev.key 0 entry.p
          in
          let path = String.concat "" [prefix; Bytes.to_string entry.k] in
          Util.ensure_valid_key path ;
          let right =
            match entry.t with
            | Some r ->
                retrieve_node_lazy t r
            | None ->
                lazy Lwt.return_none
          in
          {layer; key= path; value= entry.v; right} :: entries )
        [] node_raw.e
    in
    Lwt.return {layer; left; entries}

  (* returns the layer of a node *)
  and get_node_height t node : int Lwt.t =
    match (node.l, node.e) with
    | None, [] ->
        Lwt.return 0
    | Some left, [] -> (
        match%lwt retrieve_node_raw t left with
        | Some node ->
            let%lwt height = get_node_height t node in
            Lwt.return (height + 1)
        | None ->
            failwith ("couldn't find node " ^ Cid.to_string left) )
    | _, leaf :: _ -> (
      match leaf.p with
      | 0 ->
          Lwt.return (Util.leading_zeros_on_hash (Bytes.to_string leaf.k))
      | _ ->
          failwith "first node entry has nonzero p value" )

  (* calls fn with each entry's key and cid *)
  let traverse t fn : unit Lwt.t =
    let rec traverse node =
      let%lwt () =
        let*? left = node.left in
        match left with Some l -> traverse l | None -> Lwt.return_unit
      in
      List.iter (fun entry -> fn entry.key entry.value) node.entries ;
      Lwt.return_unit
    in
    match%lwt retrieve_node t t.root with
    | Some root ->
        traverse root
    | None ->
        failwith "root cid not found in repo store"

  (* returns a map of key -> cid *)
  let build_map t : Cid.t StringMap.t Lwt.t =
    let map = StringMap.empty in
    let%lwt () =
      traverse t (fun path cid -> ignore (StringMap.add path cid map))
    in
    Lwt.return map

  (* produces a cid and cbor-encoded bytes for this mst *)
  let serialize t map : (Cid.t * bytes) Lwt.t =
    let keys =
      map |> StringMap.bindings |> List.map fst |> List.sort String.compare
    in
    let entry_for_key key =
      let value = StringMap.find key map in
      let height = Util.leading_zeros_on_hash key in
      {layer= height; key; value; right= lazy Lwt.return_none}
    in
    let root =
      { layer= keys |> List.hd |> Util.leading_zeros_on_hash
      ; entries= []
      ; left= lazy Lwt.return_none }
    in
    List.iter
      (fun key -> ignore (insert_entry root (entry_for_key key)))
      (List.tl keys) ;
    let rec finalize node : (Cid.t * bytes) Lwt.t =
      let%lwt left =
        node.left
        >>? function
        | Some l ->
            let%lwt cid, _ = finalize l in
            Lwt.return_some cid
        | None ->
            Lwt.return_none
      in
      let last_key = ref "" in
      let%lwt mst_entries =
        Lwt_list.map_s
          (fun entry ->
            let%lwt right =
              entry.right
              >>? function
              | Some r ->
                  let%lwt cid, _ = finalize r in
                  Lwt.return (Some cid)
              | None ->
                  Lwt.return None
            in
            let prefix_len = Util.shared_prefix_length !last_key entry.key in
            last_key := entry.key ;
            Lwt.return
              { k=
                  Bytes.of_string
                    (String.sub entry.key prefix_len
                       (String.length entry.key - prefix_len) )
              ; p= prefix_len
              ; v= entry.value
              ; t= right } )
          node.entries
      in
      let encoded =
        Dag_cbor.encode (encode_node_raw {l= left; e= mst_entries})
      in
      let cid = Cid.create Dcbor encoded in
      let%lwt () = Store.put_block t.blockstore cid encoded in
      Lwt.return (cid, encoded)
    in
    finalize root

  (* returns cids and blocks that form the path from a given node to a given entry *)
  let rec path_to_entry t node key : (Cid.t * bytes) list Lwt.t =
    let%lwt root_bytes = Store.get_bytes t.blockstore node in
    let%lwt root =
      match root_bytes with
      | None ->
          Lwt.return_none
      | Some bytes ->
          Lwt.return_some (decode_block_raw bytes)
    in
    let path_tail = [(node, Option.get root_bytes)] in
    (* if there is a left child, try to find a path through the left subtree *)
    let%lwt path_through_left =
      match root with
      | None ->
          Lwt.return_some []
      | Some root -> (
        match root.l with
        | None ->
            Lwt.return_none
        | Some left -> (
            match%lwt path_to_entry t left key with
            | [] ->
                Lwt.return_none
            | path ->
                (* Option.get is safe because root is Some only when root_bytes is Some *)
                Lwt.return_some (path @ path_tail) ) )
    in
    match path_through_left with
    | Some path ->
        Lwt.return path
    | None -> (
        (* if a left subtree path couldn't be found, find the entry whose right subtree this key would belong to *)
        let entries = (Option.get root).e in
        let entries_keys = entries_to_keys entries in
        let entries_len = List.length entries in
        let entry_index =
          match List.find_index (fun e -> e >= key) entries_keys with
          | Some index ->
              index
          | None ->
              entries_len
        in
        (* entry_index here is actually the entry to the right of the subtree the key would belong to *)
        match entry_index with
        | _
        (* because entries[entry_index] might turn out to be the entry we're looking for *)
          when entry_index < entries_len
               && List.nth entries_keys entry_index = key ->
            Lwt.return path_tail
        | _ -> (
          (* otherwise, we continue down the right subtree of the entry before entry_index *)
          match Util.last entries with
          | Some last when last.t != None ->
              let%lwt path_through_right =
                path_to_entry t (Option.get last.t) key
              in
              Lwt.return (path_through_right @ path_tail)
          | _ ->
              Lwt.return path_tail ) )

  (* returns all mst entries in order for a car stream *)
  let to_blocks_seq t : (Cid.t * bytes) Lwt_seq.t =
    let module M = struct
      type stage =
        (* currently walking nodes *)
        | Nodes of
            { next: Cid.t list (* next cids to fetch *)
            ; fetched: (Cid.t * bytes) list (* fetched cids and their bytes *)
            ; leaves: Cid.Set.t (* seen leaf cids *) }
        (* done walking nodes, streaming accumulated leaves *)
        | Leaves of (Cid.t * bytes) list
        | Done
    end in
    let open M in
    let init_state =
      Nodes {next= [t.root]; fetched= []; leaves= Cid.Set.empty}
    in
    let rec step = function
      | Done ->
          Lwt.return_none
      (* node has been fetched, can now be yielded *)
      | Nodes ({fetched= (cid, bytes) :: rest; _} as s) ->
          Lwt.return_some ((cid, bytes), Nodes {s with fetched= rest})
      (* need to fetch next nodes *)
      | Nodes {next; fetched= []; leaves} ->
          if List.is_empty next then (
            (* finished traversing nodes, time to switch to leaves *)
            let leaves_list = Cid.Set.to_list leaves in
            let%lwt leaves_bm = Store.get_blocks t.blockstore leaves_list in
            if leaves_bm.missing <> [] then failwith "missing mst leaf blocks" ;
            let leaves_nodes = Storage.Block_map.entries leaves_bm.blocks in
            match leaves_nodes with
            | [] ->
                (* with Done, we don't care about the first pair element *)
                Lwt.return_some (Obj.magic (), Done)
            | _ ->
                (* it's leafin time *)
                step (Leaves leaves_nodes) )
          else
            (* go ahead and fetch the next nodes *)
            let%lwt bm = Store.get_blocks t.blockstore next in
            if bm.missing <> [] then failwith "missing mst nodes" ;
            let fetched, next', leaves' =
              List.fold_left
                (fun (acc, nxt, lvs) cid ->
                  let bytes =
                    (* we should be safe to do this since we just got the cids from the blockmap *)
                    Storage.Block_map.get cid bm.blocks |> Option.get
                  in
                  let node = decode_block_raw bytes in
                  let nxt' =
                    List.fold_left
                      (* node.entries.map(e => e.right) *)
                      (fun n e -> match e.t with Some c -> c :: n | None -> n )
                      (* start with [node.left, ...nxt] if node has a left subtree *)
                      (* next' looks like [..., n_2.r_2, n_2.l, n_1.r_n, ..., n_1.r_1, n_1.l]) *)
                      ( match node.l with
                      | Some l ->
                          l :: nxt
                      | None ->
                          nxt )
                      node.e
                  in
                  let lvs' =
                    (* add each entry in this node to the list of seen leaves *)
                    List.fold_left (fun s e -> Cid.Set.add e.v s) lvs node.e
                  in
                  (* prepending is O(1) per prepend + one O(n) to reverse, vs. O(n) per append = O(n^2) total *)
                  ((cid, bytes) :: acc, nxt', lvs') )
                ([], [], leaves) next
            in
            step
              (Nodes
                 { next= List.rev next'
                 ; fetched= List.rev fetched
                 ; leaves= leaves' } )
      (* if we're onto yielding leaves, do that *)
      | Leaves ((cid, bytes) :: rest) ->
          let next = if rest = [] then Done else Leaves rest in
          Lwt.return_some ((cid, bytes), next)
      (* once we're out of leaves, we're done *)
      | Leaves [] ->
          Lwt.return_some (Obj.magic (), Done)
    in
    Lwt_seq.unfold_lwt step init_state
end
