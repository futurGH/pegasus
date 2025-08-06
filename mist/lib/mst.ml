module StringMap = Dag_cbor.StringMap

type node_raw =
  { (* link to lower level left subtree with all keys sorting before this node *)
    l: Cid.t option
  ; (* ordered list of entries below this node *)
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

module Make (Store : Storage.Writable_blockstore) = struct
  type bs = Store.t

  type t = {blockstore: bs; root: Cid.t}

  let create blockstore root = {blockstore; root}

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

  let retrieve_node t cid : node_raw option Lwt.t =
    match%lwt Store.get_bytes t.blockstore cid with
    | Some bytes ->
        Lwt.return_some (decode_block bytes)
    | None ->
        Lwt.return_none

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

  let build_map t : Cid.t StringMap.t Lwt.t =
    let map = StringMap.empty in
    let%lwt () =
      traverse t (fun path cid -> ignore (StringMap.add path cid map))
    in
    Lwt.return map

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
      if List.length node.entries > 0 then
        assert (entry.key > (List.rev node.entries |> List.hd).key) ;
      node.entries <- node.entries @ [entry] ;
      Lwt.return node )

  let hydrate_from_map t map =
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
end
