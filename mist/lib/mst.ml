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

type node_hydrated =
  { layer: int
  ; mutable left: node_hydrated option
  ; mutable entries: entry_hydrated list }

and entry_hydrated =
  {layer: int; key: string; value: Cid.t; right: node_hydrated option}

(* figures out where to put an entry in or below a hydrated node, returns new node *)
let rec insert_entry node entry : node_hydrated Lwt.t =
  let entry_layer = Util.leading_zeros_on_hash entry.key in
  (* as long as node layer <= entry layer, create a new node above node
     until we have a node at the correct height for the entry to be inserted *)
  let rec build_insert_node node layer =
    if layer >= entry_layer then node
    else
      build_insert_node
        {layer= layer + 1; left= Some node; entries= []}
        (layer + 1)
  in
  let insert_node = build_insert_node node node.layer in
  (* if entry is below node, recursively insert into node's left subtree *)
  if entry_layer < insert_node.layer then
    match (insert_node.entries, insert_node.left) with
    | [], None ->
        failwith "found totally empty mst node"
    | [], Some left ->
        node.left <- Some (Lwt_main.run (insert_entry left entry)) ;
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

(* hydrates a list of entries with their keys; layer and right value are placeholders *)
let hydrate_entries_keys_only node =
  node.e
  |> List.fold_left
       (fun (prev_path, entries) entry ->
         let prefix = String.sub prev_path 0 entry.p in
         let path = String.concat "" [prefix; Bytes.to_string entry.k] in
         Util.ensure_valid_key path ;
         (path, entries @ [{layer= 0; key= path; value= entry.v; right= None}]) )
       ("", [])
  |> snd

module Make (Store : Storage.Writable_blockstore) = struct
  type bs = Store.t

  type t = {blockstore: bs; root: Cid.t}

  let create blockstore root = {blockstore; root}

  (* decodes a node retrieved from the blockstore *)
  let decode_block b : node_raw =
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

  (* retrieves & decodes a node by cid *)
  let retrieve_node t cid : node_raw option Lwt.t =
    match%lwt Store.get_bytes t.blockstore cid with
    | Some bytes ->
        Lwt.return_some (decode_block bytes)
    | None ->
        Lwt.return_none

  (* returns the layer of a node *)
  let rec get_node_height t node : int Lwt.t =
    match (node.l, node.e) with
    | None, [] ->
        Lwt.return 0
    | Some left, [] -> (
        match%lwt retrieve_node t left with
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
        match node.l with
        | Some cid -> (
            match%lwt retrieve_node t cid with
            | Some node ->
                traverse node
            | None ->
                Lwt.return_unit )
        | None ->
            Lwt.return_unit
      in
      ignore
        (List.fold_left
           (fun prev_path entry ->
             let prefix = String.sub prev_path 0 entry.p in
             let path = String.concat "" [prefix; Bytes.to_string entry.k] in
             fn path entry.v ; path )
           "" node.e ) ;
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

  (* produces a hydrated mst from a map of key -> cid *)
  let hydrate_from_map t map : Cid.t Lwt.t =
    let keys =
      map |> StringMap.bindings |> List.map fst |> List.sort String.compare
    in
    let entry_for_key key =
      let value = StringMap.find key map in
      let height = Util.leading_zeros_on_hash key in
      {layer= height; key; value; right= None}
    in
    let root =
      { layer= keys |> List.hd |> Util.leading_zeros_on_hash
      ; entries= []
      ; left= None }
    in
    List.iter
      (fun key -> ignore (insert_entry root (entry_for_key key)))
      (List.tl keys) ;
    let rec finalize node : Cid.t Lwt.t =
      let left =
        match node.left with
        | Some l ->
            Some (Lwt_main.run (finalize l))
        | None ->
            None
      in
      let last_key = ref "" in
      let mst_entries =
        List.map
          (fun entry ->
            let right =
              match entry.right with
              | Some r ->
                  Some (Lwt_main.run (finalize r))
              | None ->
                  None
            in
            let prefix_len = Util.shared_prefix_length !last_key entry.key in
            last_key := entry.key ;
            { k=
                Bytes.of_string
                  (String.sub entry.key prefix_len
                     (String.length entry.key - prefix_len) )
            ; p= prefix_len
            ; v= entry.value
            ; t= right } )
          node.entries
      in
      let mst_node = {l= left; e= mst_entries} in
      let encoded = Dag_cbor.encode (encode_node_raw mst_node) in
      let cid = Cid.create Dcbor encoded in
      let%lwt () = Store.put_block t.blockstore cid encoded in
      Lwt.return cid
    in
    finalize root

  (* returns cids and blocks that form the path from a given node to a given entry *)
  let rec path_to_entry t node key : (Cid.t * bytes) list Lwt.t =
    let%lwt root_bytes = Store.get_bytes t node in
    let%lwt root =
      match root_bytes with
      | None ->
          Lwt.return_none
      | Some bytes ->
          Lwt.return_some (decode_block bytes)
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
        let root' = Option.get root in
        let entries_keys = hydrate_entries_keys_only root' in
        let entries_len = List.length root'.e in
        let entry_index =
          match List.find_index (fun e -> e.key >= key) entries_keys with
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
               && (List.nth entries_keys entry_index).key = key ->
            Lwt.return path_tail
        | _ -> (
          (* otherwise, we continue down the right subtree of the entry before entry_index *)
          match Util.last root'.e with
          | Some last when last.t != None ->
              let%lwt path_through_right =
                path_to_entry t (Option.get last.t) key
              in
              Lwt.return (path_through_right @ path_tail)
          | _ ->
              Lwt.return path_tail ) )

  (* returns all mst entries in order for a car stream *)
  let to_car_stream t : (Cid.t * bytes) Seq.t =
    let module M = struct
      type stage =
        | Nodes of
            (* currently walking nodes *)
            
            { next: Cid.t list (* next cids to fetch *)
            ; fetched: (Cid.t * bytes) list (* fetched cids and their bytes *)
            ; leaves: Cid.Set.t (* seen leaf cids *) }
        | Leaves of
            (* done walking nodes, streaming accumulated leaves *)
            (Cid.t * bytes) list
        | Done
    end in
    let open M in
    let init_state =
      Nodes {next= [t.root]; fetched= []; leaves= Cid.Set.empty}
    in
    let rec step = function
      | Done ->
          None
      (* node has been fetched, can now be yielded *)
      | Nodes ({fetched= (cid, bytes) :: rest; _} as s) ->
          Some ((cid, bytes), Nodes {s with fetched= rest})
      (* need to fetch next nodes *)
      | Nodes {next; fetched= []; leaves} ->
          if List.is_empty next then (
            (* finished traversing nodes, time to switch to leaves *)
            let leaves_list = Cid.Set.to_list leaves in
            let leaves_bm =
              Lwt_main.run (Store.get_blocks t.blockstore leaves_list)
            in
            if leaves_bm.missing <> [] then failwith "missing mst leaf blocks" ;
            let leaves_nodes = Storage.Block_map.entries leaves_bm.blocks in
            match leaves_nodes with
            | [] ->
                (* with Done, we don't care about the first pair element *)
                Some (Obj.magic (), Done)
            | _ ->
                (* it's leafin time *)
                step (Leaves leaves_nodes) )
          else
            (* go ahead and fetch the next nodes *)
            let bm = Lwt_main.run (Store.get_blocks t.blockstore next) in
            if bm.missing <> [] then failwith "missing mst nodes" ;
            let fetched, next', leaves' =
              List.fold_left
                (fun (acc, nxt, lvs) cid ->
                  let bytes =
                    (* we should be safe to do this since we just got the cids from the blockmap *)
                    Storage.Block_map.get cid bm.blocks |> Option.get
                  in
                  let node = decode_block bytes in
                  let nxt' =
                    List.fold_left
                      (* node.entries.map(e => e.right) *)
                      (fun n e -> match e.t with Some c -> c :: n | None -> n )
                      (* start with [node.left, ...nxt] if node has a left subtree *)
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
          Some ((cid, bytes), next)
      (* once we're out of leaves, we're done *)
      | Leaves [] ->
          Some (Obj.magic (), Done)
    in
    Seq.unfold step init_state
end
