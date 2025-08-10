open Storage
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

type node_or_entry = Node of node | Entry of entry

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

(* returns the index of the first entry in an interspersed list that's gte a given key *)
let find_gte_entry_index entries key : int =
  let rec aux entries index =
    match entries with
    | [] ->
        (* will be entries length when not found *)
        index
    | e :: es -> (
      match e with
      | Entry entry when entry.key >= key ->
          index
      | _ ->
          aux es (index + 1) )
  in
  aux entries 0

(* produces a cid and cbor-encoded bytes for a given tree *)
let serialize node : (Cid.t * bytes) Lwt.t =
  let sorted_entries =
    List.sort (fun a b -> String.compare a.key b.key) node.entries
  in
  let rec aux node =
    let%lwt left =
      node.left
      >>? function
      | Some l ->
          let%lwt cid, _ = aux l in
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
                let%lwt cid, _ = aux r in
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
    let encoded = Dag_cbor.encode (encode_node_raw {l= left; e= mst_entries}) in
    let cid = Cid.create Dcbor encoded in
    Lwt.return (cid, encoded)
  in
  aux {node with entries= sorted_entries}

module Make (Store : Writable_blockstore) = struct
  type bs = Store.t

  type t = {blockstore: bs; root: Cid.t}

  let create blockstore root = {blockstore; root}

  (* decodes a node retrieved from the blockstore *)
  let decode_block_raw b : node_raw =
    match Dag_cbor.decode b with
    | `Map node ->
        Yojson.Safe.pretty_print Format.std_formatter
          (Dag_cbor.to_yojson (`Map node)) ;
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

  (* retrieves a raw node by cid *)
  let retrieve_node_raw t cid : node_raw option Lwt.t =
    match%lwt Store.get_bytes t.blockstore cid with
    | Some bytes ->
        bytes |> decode_block_raw |> Lwt.return_some
    | None ->
        Lwt.return_none

  (* retrieves & hydrates a node by cid *)
  let rec retrieve_node t cid : node option Lwt.t =
    match%lwt retrieve_node_raw t cid with
    | Some raw ->
        hydrate_node t raw |> Lwt.map Option.some
    | None ->
        Lwt.return_none

  (* lazy version of retrieve_node *)
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

  (* returns cids and blocks that form the path from a given node to a given entry *)
  let rec path_to_entry t node key : (Cid.t * bytes) list Lwt.t =
    let%lwt root_bytes = Store.get_bytes t.blockstore node in
    let%lwt root_raw =
      match root_bytes with
      | None ->
          Lwt.return_none
      | Some bytes ->
          Lwt.return_some (decode_block_raw bytes)
    in
    let%lwt root =
      match root_raw with
      | None ->
          Lwt.return_none
      | Some root ->
          hydrate_node t root |> Lwt.map Option.some
    in
    (* if there is a left child, try to find a path through the left subtree *)
    let%lwt path_through_left =
      match root_raw with
      | None ->
          Lwt.return_some []
      | Some raw -> (
        match raw.l with
        | None ->
            Lwt.return_none
        | Some left -> (
            match%lwt path_to_entry t left key with
            | [] ->
                Lwt.return_none
            | path ->
                (* Option.get is safe because root is Some only when root_bytes is Some *)
                Lwt.return_some (path @ [(node, Option.get root_bytes)]) ) )
    in
    match path_through_left with
    | Some path ->
        Lwt.return path
    | None -> (
        (* if a left subtree path couldn't be found, find the entry whose right subtree this key would belong to *)
        (* this branch is only reached when root/root_raw/root_bytes are not None;
        if they were, path_through_left would be Some [] *)
        let entries = (Option.get root).entries in
        let entries_len = List.length entries in
        let entry_index =
          match List.find_index (fun e -> e.key >= key) entries with
          | Some index ->
              index
          | None ->
              entries_len
        in
        (* path_through_left is None -> root_bytes is Some *)
        let path_tail = [(node, Option.get root_bytes)] in
        (* entry_index here is actually the entry to the right of the subtree the key would belong to *)
        match entry_index with
        | _
        (* because entries[entry_index] might turn out to be the entry we're looking for *)
          when entry_index < entries_len
               && (List.nth entries entry_index).key = key ->
            Lwt.return path_tail
        | _ -> (
          (* otherwise, we continue down the right subtree of the entry before entry_index *)
          (* path_through_left is None -> root_raw is Some *)
          match Util.last (Option.get root_raw).e with
          | Some last when last.t <> None ->
              let%lwt path_through_right =
                (* when last.t <> None *)
                path_to_entry t (Option.get last.t) key
              in
              Lwt.return (path_through_right @ path_tail)
          | _ ->
              Lwt.return path_tail ) )

  (* returns all mst entries in order for a car stream *)
  let to_blocks_stream t : (Cid.t * bytes) Lwt_seq.t =
    let module M = struct
      type stage =
        (* currently walking nodes *)
        | Nodes of
            { next: Cid.t list (* next cids to fetch *)
            ; fetched: (Cid.t * bytes) list (* fetched cids and their bytes *)
            ; leaves_seen: Cid.Set.t (* seen leaf cids for dedupe *)
            ; leaves_rev: Cid.t list (* reversed encounter order of leaves *) }
        (* done walking nodes, streaming accumulated leaves *)
        | Leaves of (Cid.t * bytes) list
        | Done
    end in
    let open M in
    let init_state =
      Nodes
        {next= [t.root]; fetched= []; leaves_seen= Cid.Set.empty; leaves_rev= []}
    in
    let rec step = function
      | Done ->
          Lwt.return_none
      (* node has been fetched, can now be yielded *)
      | Nodes ({fetched= (cid, bytes) :: rest; _} as s) ->
          Lwt.return_some ((cid, bytes), Nodes {s with fetched= rest})
      (* need to fetch next nodes *)
      | Nodes {next; fetched= []; leaves_seen; leaves_rev} ->
          if List.is_empty next then (
            (* finished traversing nodes, time to switch to leaves *)
            let leaves_list = List.rev leaves_rev in
            let%lwt leaves_bm = Store.get_blocks t.blockstore leaves_list in
            if leaves_bm.missing <> [] then failwith "missing mst leaf blocks" ;
            let leaves_nodes =
              List.map
                (fun cid ->
                  let bytes =
                    Block_map.get cid leaves_bm.blocks |> Option.get
                  in
                  (cid, bytes) )
                leaves_list
            in
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
            let fetched, next', leaves_seen', leaves_rev' =
              List.fold_left
                (fun (acc, nxt, seen, rev) cid ->
                  let bytes =
                    (* we should be safe to do this since we just got the cids from the blockmap *)
                    Block_map.get cid bm.blocks |> Option.get
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
                  let seen', rev' =
                    (* add each entry in this node to the seen set and record encounter order *)
                    List.fold_left
                      (fun (s, r) e ->
                        if Cid.Set.mem e.v s then (s, r)
                        else (Cid.Set.add e.v s, e.v :: r) )
                      (seen, rev) node.e
                  in
                  (* prepending is O(1) per prepend + one O(n) to reverse, vs. O(n) per append = O(n^2) total *)
                  ((cid, bytes) :: acc, nxt', seen', rev') )
                ([], [], leaves_seen, leaves_rev)
                next
            in
            step
              (Nodes
                 { next= List.rev next'
                 ; fetched= List.rev fetched
                 ; leaves_seen= leaves_seen'
                 ; leaves_rev= leaves_rev' } )
      (* if we're onto yielding leaves, do that *)
      | Leaves ((cid, bytes) :: rest) ->
          let next = if rest = [] then Done else Leaves rest in
          Lwt.return_some ((cid, bytes), next)
      (* once we're out of leaves, we're done *)
      | Leaves [] ->
          Lwt.return_some (Obj.magic (), Done)
    in
    Lwt_seq.unfold_lwt step init_state

  (* returns a car v1 formatted stream containing the mst *)
  let to_car_stream t : bytes Lwt_seq.t =
    t |> to_blocks_stream |> Car.blocks_to_stream (Some t.root)

  (* returns a car archive containing the mst *)
  let to_car t : bytes Lwt.t =
    t |> to_blocks_stream |> Car.blocks_to_car (Some t.root)

  (* returns all mst nodes needed to prove the value of a given key *)
  let rec proof_for_key t root key : Block_map.t Lwt.t =
    let e_rev = List.rev root.entries in
    (* iterate in reverse because if the key doesn't exist at this level,
       we need to search the "previous" node's right subtree *)
    let rec find_proof entries_rev =
      match entries_rev with
      | [] ->
          Lwt.return Block_map.empty
      | e :: rest -> (
          if e.key > key then find_proof rest
          else if e.key = key then Lwt.return Block_map.empty
          else
            let*? right = e.right in
            match right with
            | Some r ->
                proof_for_key t r key
            | None ->
                Lwt.return Block_map.empty )
    in
    let%lwt bm = find_proof e_rev in
    let%lwt root_cid, root_bytes = serialize root in
    Lwt.return (Block_map.set root_cid root_bytes bm)

  (* returns all mst nodes needed to prove the value of a given key's left sibling *)
  let rec proof_for_left_sibling t root key : Block_map.t Lwt.t =
    let e_rev = List.rev root.entries in
    (* iterate in reverse for the same reason as proof_for_key *)
    let rec find_proof entries_rev =
      match entries_rev with
      | [] ->
          Lwt.return Block_map.empty
      | e :: rest -> (
          if e.key >= key then find_proof rest
          else
            let*? right = e.right in
            match right with
            | Some r ->
                proof_for_left_sibling t r key
            | None ->
                Lwt.return Block_map.empty )
    in
    let%lwt bm = find_proof e_rev in
    let%lwt root_cid, root_bytes = serialize root in
    Lwt.return (Block_map.set root_cid root_bytes bm)

  (* returns all mst nodes needed to prove the value of a given key's right sibling *)
  let rec proof_for_right_sibling t root key : Block_map.t Lwt.t =
    (* unlike the other two, this doesn't iterate in reverse
       because we can stop as soon as we're past the key *)
    let rec find_proof ?(prev = None) entries =
      match entries with
      | [] -> (
        (* end of entries, right sibling must be in the last entry's right subtree *)
        match prev with
        | Some e -> (
            let*? right = e.right in
            match right with
            | Some r ->
                proof_for_right_sibling t r key
            | None ->
                Lwt.return Block_map.empty )
        | None ->
            Lwt.return Block_map.empty )
      | e :: rest ->
          if e.key > key then
            (* we're past target key; right sibling is in previous entry's right subtree *)
            match prev with
            | Some p -> (
                let*? right = p.right in
                match right with
                | Some r ->
                    proof_for_right_sibling t r key
                (* I don't think this should ever happen? *)
                | None ->
                    Lwt.return Block_map.empty )
            (* first entry is already greater than key; we're inside the sibling *)
            | None ->
                Lwt.return Block_map.empty
          else if e.key = key then
            (* found the entry, right sibling is in its right subtree *)
            let*? right = e.right in
            match right with
            | Some r ->
                proof_for_right_sibling t r key
            | None ->
                Lwt.return Block_map.empty
          else (* e.key < key, keep searching *)
            find_proof ~prev:(Some e) rest
    in
    let%lwt bm = find_proof root.entries in
    let%lwt root_cid, root_bytes = serialize root in
    Lwt.return (Block_map.set root_cid root_bytes bm)

  (* a covering proof is all mst nodes needed to prove the value of a given leaf
    and its siblings to its immediate right and left (if applicable) *)
  let get_covering_proof t root key : Block_map.t Lwt.t =
    let%lwt proofs =
      Lwt.all
        [ proof_for_key t root key
        ; proof_for_left_sibling t root key
        ; proof_for_right_sibling t root key ]
    in
    Lwt.return
      (List.fold_left
         (fun acc proof -> Block_map.merge acc proof)
         Block_map.empty proofs )

  (*** diffs ***)
  type diff_add = {key: string; cid: Cid.t}

  type diff_update = {key: string; prev: Cid.t; cid: Cid.t}

  type diff_delete = {key: string; cid: Cid.t}

  type data_diff =
    { adds: diff_add list
    ; updates: diff_update list
    ; deletes: diff_delete list
    ; new_mst_blocks: (Cid.t * bytes) list
    ; new_leaf_cids: Cid.Set.t
    ; removed_cids: Cid.Set.t }

  (* collects all node blocks (cid, bytes) and all leaf cids reachable from root
     only traverses nodes; doesn't fetch leaf blocks
     returns (nodes, visited, leaves) *)
  let collect_nodes_and_leaves (t : t) :
      ((Cid.t * bytes) list * Cid.Set.t * Cid.Set.t) Lwt.t =
    let rec bfs (queue : Cid.t list) (visited : Cid.Set.t)
        (nodes : (Cid.t * bytes) list) (leaves : Cid.Set.t) =
      match queue with
      | [] ->
          Lwt.return (nodes, visited, leaves)
      | cid :: rest -> (
          if Cid.Set.mem cid visited then bfs rest visited nodes leaves
          else
            let%lwt bytes_opt = Store.get_bytes t.blockstore cid in
            match bytes_opt with
            | None ->
                failwith ("missing mst node block: " ^ Cid.to_string cid)
            | Some bytes ->
                let raw = decode_block_raw bytes in
                (* queue subtrees *)
                let next_cids =
                  let acc = match raw.l with Some l -> [l] | None -> [] in
                  List.fold_left
                    (fun acc e ->
                      match e.t with Some r -> r :: acc | None -> acc )
                    acc raw.e
                in
                (* accumulate leaf cids *)
                let leaves' =
                  List.fold_left (fun s e -> Cid.Set.add e.v s) leaves raw.e
                in
                let visited' = Cid.Set.add cid visited in
                bfs
                  (List.rev_append next_cids rest)
                  visited' ((cid, bytes) :: nodes) leaves' )
    in
    bfs [t.root] Cid.Set.empty [] Cid.Set.empty

  (* list of all leaves belonging to a node, ordered by key *)
  let rec leaves_of_node (n : node) : (string * Cid.t) list Lwt.t =
    let%lwt left_leaves =
      n.left >>? function Some l -> leaves_of_node l | None -> Lwt.return []
    in
    let sorted_entries =
      List.sort
        (fun (a : entry) (b : entry) -> String.compare a.key b.key)
        n.entries
    in
    let%lwt leaves =
      Lwt_list.fold_left_s
        (fun acc e ->
          let%lwt right_leaves =
            e.right
            >>? function Some r -> leaves_of_node r | None -> Lwt.return []
          in
          Lwt.return (acc @ [(e.key, e.value)] @ right_leaves) )
        left_leaves sorted_entries
    in
    Lwt.return leaves

  (* little helper *)
  let leaves_of_root (t : t) : (string * Cid.t) list Lwt.t =
    match%lwt retrieve_node t t.root with
    | None ->
        failwith "root cid not found in repo store"
    | Some root ->
        leaves_of_node root

  (* produces a diff from an empty mst to the current one *)
  let null_diff (curr : t) : data_diff Lwt.t =
    let%lwt curr_nodes, _, curr_leaf_set = collect_nodes_and_leaves curr in
    let%lwt curr_leaves = leaves_of_root curr in
    let adds = List.map (fun (key, cid) : diff_add -> {key; cid}) curr_leaves in
    Lwt.return
      { adds
      ; updates= []
      ; deletes= []
      ; new_mst_blocks= curr_nodes
      ; new_leaf_cids= curr_leaf_set
      ; removed_cids= Cid.Set.empty }

  (* produces a diff between two msts *)
  let mst_diff (t_curr : t) (t_prev_opt : t option) : data_diff Lwt.t =
    match t_prev_opt with
    | None ->
        null_diff t_curr
    | Some t_prev ->
        let%lwt curr_nodes, curr_node_set, curr_leaf_set =
          collect_nodes_and_leaves t_curr
        in
        let%lwt _, prev_node_set, prev_leaf_set =
          collect_nodes_and_leaves t_prev
        in
        (* just convenient to have these functions *)
        let in_prev_nodes cid = Cid.Set.mem cid prev_node_set in
        let in_curr_nodes cid = Cid.Set.mem cid curr_node_set in
        let in_prev_leaves cid = Cid.Set.mem cid prev_leaf_set in
        let in_curr_leaves cid = Cid.Set.mem cid curr_leaf_set in
        (* new mst blocks are curr nodes that are not in prev *)
        let new_mst_blocks =
          List.filter (fun (cid, _) -> not (in_prev_nodes cid)) curr_nodes
        in
        (* removed cids are prev nodes not in curr plus prev leaves not in curr *)
        let removed_node_cids =
          Cid.Set.fold
            (fun cid acc ->
              if not (in_curr_nodes cid) then Cid.Set.add cid acc else acc )
            prev_node_set Cid.Set.empty
        in
        let removed_leaf_cids =
          Cid.Set.fold
            (fun cid acc ->
              if not (in_curr_leaves cid) then Cid.Set.add cid acc else acc )
            prev_leaf_set Cid.Set.empty
        in
        let removed_cids = Cid.Set.union removed_node_cids removed_leaf_cids in
        (* new leaf cids are curr leaves not in prev *)
        let new_leaf_cids =
          Cid.Set.fold
            (fun cid acc ->
              if not (in_prev_leaves cid) then Cid.Set.add cid acc else acc )
            curr_leaf_set Cid.Set.empty
        in
        (* compute adds/updates/deletes by merging sorted leaves *)
        let%lwt curr_leaves = leaves_of_root t_curr in
        let%lwt prev_leaves = leaves_of_root t_prev in
        let rec merge (pl : (string * Cid.t) list) (cl : (string * Cid.t) list)
            (adds : diff_add list) (updates : diff_update list)
            (deletes : diff_delete list) =
          match (pl, cl) with
          | [], [] ->
              (* we prepend for speed, then reverse at the end *)
              (List.rev adds, List.rev updates, List.rev deletes)
          | [], (k, c) :: cr ->
              (* more curr than prev, goes in adds *)
              merge [] cr ({key= k; cid= c} :: adds) updates deletes
          | (k, c) :: pr, [] ->
              (* more prev than curr, goes in deletes *)
              merge pr [] adds updates ({key= k; cid= c} :: deletes)
          | (k1, c1) :: pr, (k2, c2) :: cr ->
              if k1 = k2 then (* if key & value are the same, keep going *)
                if Cid.equal c1 c2 then merge pr cr adds updates deletes
                else (* same key, different value; update *)
                  merge pr cr adds
                    ({key= k1; prev= c1; cid= c2} :: updates)
                    deletes
              else if k1 < k2 then
                merge pr ((k2, c2) :: cr) adds updates
                  ({key= k1; cid= c1} :: deletes)
              else
                merge ((k1, c1) :: pr) cr
                  ({key= k2; cid= c2} :: adds)
                  updates deletes
        in
        let adds, updates, deletes = merge prev_leaves curr_leaves [] [] [] in
        Lwt.return
          {adds; updates; deletes; new_mst_blocks; new_leaf_cids; removed_cids}

  (* checks that two msts are identical by recursively comparing their entries *)
  let equal (t1 : t) (t2 : t) : bool Lwt.t =
    let rec nodes_equal (n1 : node) (n2 : node) : bool Lwt.t =
      if n1.layer <> n2.layer then Lwt.return false
      else if List.length n1.entries <> List.length n2.entries then
        Lwt.return false
      else
        let%lwt left_equal =
          n1.left
          >>? function
          | Some l1 -> (
              n2.left
              >>? function
              | Some l2 ->
                  nodes_equal l1 l2
              | None ->
                  Lwt.return false )
          | None -> (
              n2.left
              >>? function
              | Some _ ->
                  Lwt.return false
              | None ->
                  Lwt.return true )
        in
        if not left_equal then Lwt.return false
        else
          let rec entries_equal (e1s : entry list) (e2s : entry list) =
            match (e1s, e2s) with
            | [], [] ->
                Lwt.return true
            | e1 :: rest1, e2 :: rest2 ->
                if
                  e1.layer <> e2.layer || e1.key <> e2.key
                  || not (Cid.equal e1.value e2.value)
                then Lwt.return false
                else
                  let%lwt right_equal =
                    e1.right
                    >>? function
                    | Some r1 -> (
                        e2.right
                        >>? function
                        | Some r2 ->
                            nodes_equal r1 r2
                        | None ->
                            Lwt.return false )
                    | None -> (
                        e2.right
                        >>? function
                        | Some _ ->
                            Lwt.return false
                        | None ->
                            Lwt.return true )
                  in
                  if not right_equal then Lwt.return false
                  else entries_equal rest1 rest2
            | _ ->
                Lwt.return false
          in
          entries_equal n1.entries n2.entries
    in
    match%lwt Lwt.all [retrieve_node t1 t1.root; retrieve_node t2 t2.root] with
    | [Some r1; Some r2] ->
        nodes_equal r1 r2
    | [None; None] ->
        Lwt.return true
    | _ ->
        Lwt.return false
end
