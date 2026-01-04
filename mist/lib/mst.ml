open Storage
module String_map = Dag_cbor.String_map

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
    (String_map.of_list
       [ ("p", `Integer (Int64.of_int entry.p))
       ; ("k", `Bytes entry.k)
       ; ("v", `Link entry.v)
       ; ("t", match entry.t with Some t -> `Link t | None -> `Null) ] )

let encode_node_raw node : Dag_cbor.value =
  `Map
    (String_map.of_list
       [ ("l", match node.l with Some l -> `Link l | None -> `Null)
       ; ("e", `Array (Array.of_list (List.map encode_entry_raw node.e))) ] )

(* decodes a node from cbor bytes *)
let decode_block_raw b : node_raw =
  match Dag_cbor.decode b with
  | `Map node ->
      if not (String_map.mem "e" node) then
        raise (Invalid_argument "mst node missing 'e'") ;
      let l =
        if String_map.mem "l" node then
          match String_map.find "l" node with `Link l -> Some l | _ -> None
        else None
      in
      let e_array =
        match String_map.find "e" node with `Array e -> e | _ -> [||]
      in
      let e =
        Array.to_list
        @@ Array.map
             (fun (entry : Dag_cbor.value) ->
               match entry with
               | `Map entry ->
                   { p=
                       ( entry |> String_map.find "p"
                       |> function
                       | `Integer p ->
                           Int64.to_int p
                       | _ ->
                           raise (Invalid_argument "mst entry missing 'p'") )
                   ; k=
                       ( entry |> String_map.find "k"
                       |> function
                       | `Bytes k ->
                           k
                       | _ ->
                           raise (Invalid_argument "mst entry missing 'k'") )
                   ; v=
                       ( entry |> String_map.find "v"
                       |> function
                       | `Link v ->
                           v
                       | _ ->
                           raise (Invalid_argument "mst entry missing 'v'") )
                   ; t=
                       ( entry |> String_map.find "t"
                       |> function `Link t -> Some t | _ -> None ) }
               | _ ->
                   raise (Invalid_argument "non-map mst entry") )
             e_array
      in
      {l; e}
  | _ ->
      raise (Invalid_argument "invalid block")

(* items yielded by ordered stream; either an mst node block or a record cid *)
type ordered_item = Node of Cid.t * bytes | Leaf of Cid.t

type node =
  { layer: int
  ; mutable left: node option Lwt.t Lazy.t
  ; mutable entries: entry list }

and entry =
  {layer: int; key: string; value: Cid.t; right: node option Lwt.t Lazy.t}

type node_or_entry = Node of node | Entry of entry

let ( >>? ) lazy_opt_lwt f =
  let%lwt result = Lazy.force lazy_opt_lwt in
  f result

(* extracts leaves from a block map *)
let leaves_from_blocks (blocks : Block_map.t) (root : Cid.t) :
    (string * Cid.t) list =
  let leaves = ref [] in
  let stack = Stack.create () in
  Stack.push (root, "") stack ;
  while not (Stack.is_empty stack) do
    let cid, prefix = Stack.pop stack in
    match Block_map.get cid blocks with
    | None ->
        () (* missing block probably a record *)
    | Some bytes -> (
      try
        let node = decode_block_raw bytes in
        (* proess left subtree *)
        ( match node.l with
        | Some left_cid ->
            Stack.push (left_cid, prefix) stack
        | None ->
            () ) ;
        (* process entries in reverse order so they come out in correct order *)
        let last_key = ref prefix in
        List.iter
          (fun (entry : entry_raw) ->
            let key_prefix =
              if entry.p = 0 then ""
              else if entry.p <= String.length !last_key then
                String.sub !last_key 0 entry.p
              else !last_key
            in
            let full_key = key_prefix ^ Bytes.to_string entry.k in
            last_key := full_key ;
            leaves := (full_key, entry.v) :: !leaves ;
            (* push right subtree to stack *)
            match entry.t with
            | Some right_cid ->
                Stack.push (right_cid, full_key) stack
            | None ->
                () )
          node.e
      with Invalid_argument _ -> () )
  done ;
  List.sort (fun (k1, _) (k2, _) -> String.compare k1 k2) !leaves

(* extracts just mst node cids (non-leaf blocks) from a block map *)
let mst_node_cids_from_blocks (blocks : Block_map.t) (root : Cid.t) : Cid.t list
    =
  let nodes = ref [] in
  let visited = ref Cid.Set.empty in
  let stack = Stack.create () in
  Stack.push root stack ;
  while not (Stack.is_empty stack) do
    let cid = Stack.pop stack in
    if not (Cid.Set.mem cid !visited) then (
      visited := Cid.Set.add cid !visited ;
      match Block_map.get cid blocks with
      | None ->
          ()
      | Some bytes -> (
        try
          let node = decode_block_raw bytes in
          nodes := cid :: !nodes ;
          (* add all children to stack *)
          ( match node.l with
          | Some left_cid ->
              Stack.push left_cid stack
          | None ->
              () ) ;
          List.iter
            (fun (entry : entry_raw) ->
              match entry.t with
              | Some right_cid ->
                  Stack.push right_cid stack
              | None ->
                  () )
            node.e
        with Invalid_argument _ -> () ) )
  done ;
  !nodes

module type Intf = sig
  module Store : Writable_blockstore

  type t = {blockstore: Store.t; root: Cid.t}

  val create : Store.t -> Cid.t -> t

  val retrieve_node_raw : t -> Cid.t -> node_raw option Lwt.t

  val retrieve_node : ?layer_hint:int -> t -> Cid.t -> node option Lwt.t

  val retrieve_node_lazy :
    layer_hint:int -> t -> Cid.t -> node option Lwt.t lazy_t

  val get_node_height : ?layer_hint:int -> t -> node_raw -> int Lwt.t

  val traverse : t -> (string -> Cid.t -> unit) -> unit Lwt.t

  val build_map : t -> Cid.t String_map.t Lwt.t

  val to_blocks_stream : t -> (Cid.t * bytes) Lwt_seq.t

  val to_ordered_stream : t -> ordered_item Lwt_seq.t

  val serialize : t -> node -> (Cid.t * bytes, exn) Lwt_result.t

  val proof_for_key : t -> Cid.t -> string -> Block_map.t Lwt.t

  val proof_for_keys : t -> Cid.t -> string list -> Block_map.t Lwt.t

  val leaf_count : t -> int Lwt.t

  val layer : t -> int Lwt.t

  val all_nodes : t -> (Cid.t * bytes) list Lwt.t

  val create_empty : Store.t -> (t, exn) Lwt_result.t

  val of_assoc : Store.t -> (string * Cid.t) list -> t Lwt.t

  val add : t -> string -> Cid.t -> t Lwt.t

  val delete : t -> string -> t Lwt.t

  val add_rebuild : t -> string -> Cid.t -> t Lwt.t

  val delete_rebuild : t -> string -> t Lwt.t

  val collect_nodes_and_leaves :
    t -> ((Cid.t * bytes) list * Cid.Set.t * Cid.Set.t) Lwt.t

  val leaves_of_node : node -> (string * Cid.t) list Lwt.t

  val leaves_of_root : t -> (string * Cid.t) list Lwt.t

  val equal : t -> t -> bool Lwt.t
end

module Make (Store : Writable_blockstore) : Intf with module Store = Store =
struct
  module Store = Store

  type bs = Store.t

  type t = {blockstore: bs; root: Cid.t}

  let create blockstore root = {blockstore; root}

  let entries_are_sorted (entries : entry list) : bool =
    let rec aux prev_key = function
      | [] ->
          true
      | e :: tl ->
          if String.compare prev_key e.key <= 0 then aux e.key tl else false
    in
    match entries with [] -> true | e :: tl -> aux e.key tl

  (* we try to batch reads from the blockstore when possible
     200 seems like a sane upper limit for that *)
  let batch_size = 200

  let take_n n lst =
    if n <= 0 then ([], lst)
    else
      let rec loop acc remaining xs =
        match (remaining, xs) with
        | 0, _ ->
            (List.rev acc, xs)
        | _, [] ->
            (List.rev acc, [])
        | _, x :: xs' ->
            loop (x :: acc) (remaining - 1) xs'
      in
      loop [] n lst

  let get_blocks_exn (t : t) (cids : Cid.t list) : Block_map.t Lwt.t =
    if List.is_empty cids then Lwt.return Block_map.empty
    else
      let%lwt bm = Store.get_blocks t.blockstore cids in
      match bm.missing with
      | [] ->
          Lwt.return bm.blocks
      | missing :: _ ->
          failwith ("missing mst node block: " ^ Cid.to_string missing)

  (* retrieves a raw node by cid *)
  let retrieve_node_raw t cid : node_raw option Lwt.t =
    match%lwt Store.get_bytes t.blockstore cid with
    | Some bytes ->
        bytes |> decode_block_raw |> Lwt.return_some
    | None ->
        Lwt.return_none

  (* returns the layer of a node, using hint if provided *)
  let rec get_node_height ?layer_hint t node : int Lwt.t =
    match layer_hint with
    | Some layer ->
        Lwt.return layer
    | None -> (
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
            failwith "first node entry has nonzero p value" ) )

  (* hydrates a raw node *)
  let rec hydrate_node ?layer_hint t node_raw : node Lwt.t =
    let%lwt layer = get_node_height ?layer_hint t node_raw in
    let child_layer = layer - 1 in
    let left =
      match node_raw.l with
      | Some l ->
          retrieve_node_lazy ~layer_hint:child_layer t l
      | None ->
          lazy Lwt.return_none
    in
    let last_key = ref "" in
    let entries =
      List.map
        (fun entry ->
          let prefix =
            if entry.p = 0 then ""
            else if !last_key = "" then ""
            else String.sub !last_key 0 entry.p
          in
          let path = String.concat "" [prefix; Bytes.to_string entry.k] in
          Util.ensure_valid_key path ;
          last_key := path ;
          let right =
            match entry.t with
            | Some r ->
                retrieve_node_lazy ~layer_hint:child_layer t r
            | None ->
                lazy Lwt.return_none
          in
          ({layer; key= path; value= entry.v; right} : entry) )
        node_raw.e
    in
    Lwt.return {layer; left; entries}

  (* retrieves & hydrates a node by cid *)
  and retrieve_node ?layer_hint t cid : node option Lwt.t =
    match%lwt retrieve_node_raw t cid with
    | Some raw ->
        hydrate_node ?layer_hint t raw |> Lwt.map Option.some
    | None ->
        Lwt.return_none

  and retrieve_node_lazy ~layer_hint t cid =
    lazy (retrieve_node ~layer_hint t cid)

  (* calls fn with each entry's key and cid *)
  let traverse t fn : unit Lwt.t =
    let rec traverse node =
      let%lwt () =
        let%lwt left = Lazy.force node.left in
        match left with Some l -> traverse l | None -> Lwt.return_unit
      in
      let%lwt () =
        Lwt_list.iter_s
          (fun (entry : entry) ->
            fn entry.key entry.value ;
            let%lwt right = Lazy.force entry.right in
            match right with Some r -> traverse r | None -> Lwt.return_unit )
          node.entries
      in
      Lwt.return_unit
    in
    match%lwt retrieve_node t t.root with
    | Some root ->
        traverse root
    | None ->
        failwith "root cid not found in repo store"

  (* returns a map of key -> cid *)
  let build_map t : Cid.t String_map.t Lwt.t =
    let map = ref String_map.empty in
    let rec loop queue visited =
      match queue with
      | [] ->
          Lwt.return !map
      | _ ->
          let batch, rest = take_n batch_size queue in
          let to_fetch =
            List.filter (fun (cid, _) -> not (Cid.Set.mem cid visited)) batch
          in
          let%lwt blocks = get_blocks_exn t (List.map fst to_fetch) in
          let visited', next_queue =
            List.fold_left
              (fun (visited, queue) (cid, prefix) ->
                if Cid.Set.mem cid visited then (visited, queue)
                else
                  let bytes = Block_map.get cid blocks |> Option.get in
                  let raw = decode_block_raw bytes in
                  let last_key = ref prefix in
                  let next_pairs =
                    List.fold_left
                      (fun acc (e : entry_raw) ->
                        let key_prefix =
                          if e.p = 0 then ""
                          else if e.p <= String.length !last_key then
                            String.sub !last_key 0 e.p
                          else !last_key
                        in
                        let full_key = key_prefix ^ Bytes.to_string e.k in
                        Util.ensure_valid_key full_key ;
                        last_key := full_key ;
                        map := String_map.add full_key e.v !map ;
                        match e.t with
                        | Some r ->
                            (r, full_key) :: acc
                        | None ->
                            acc )
                      ( match raw.l with
                      | Some l ->
                          [(l, prefix)]
                      | None ->
                          [] )
                      raw.e
                  in
                  (Cid.Set.add cid visited, List.rev_append next_pairs queue) )
              (visited, rest) batch
          in
          loop next_queue visited'
    in
    loop [(t.root, "")] Cid.Set.empty

  (* returns all non-leaf mst node blocks in order for a car stream
     leaf cids can be obtained via collect_nodes_and_leaves or leaves_of_root *)
  let to_blocks_stream t : (Cid.t * bytes) Lwt_seq.t =
    (* (next cids to fetch list, fetched (cid * bytes) list) *)
    let init_state = ([t.root], []) in
    let rec step (next, fetched) =
      match fetched with
      (* node has been fetched, can now be yielded *)
      | (cid, bytes) :: rest ->
          Lwt.return_some ((cid, bytes), (next, rest))
      (* need to fetch next nodes *)
      | [] ->
          if List.is_empty next then Lwt.return_none
          else
            (* go ahead and fetch the next nodes *)
            let%lwt bm = Store.get_blocks t.blockstore next in
            if bm.missing <> [] then failwith "missing mst nodes" ;
            let fetched', next' =
              List.fold_left
                (fun (acc, nxt) cid ->
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
                  (* prepending then reversing is O(2n), appending each time is O(n^2) *)
                  ((cid, bytes) :: acc, nxt') )
                ([], []) next
            in
            step (List.rev next', List.rev fetched')
    in
    Lwt_seq.unfold_lwt step init_state

  (* depth-first pre-order as per sync 1.1, yields cid references in place of leaf nodes
     for each node: node block, left subtree, then for each entry: record, right subtree *)
  let to_ordered_stream t : ordered_item Lwt_seq.t =
    (* queue items: `Node cid to visit, `Leaf cid to yield *)
    let prefetch queue cache missing =
      let rec collect acc seen remaining = function
        | [] ->
            (List.rev acc, seen)
        | _ when remaining = 0 ->
            (List.rev acc, seen)
        | `Node cid :: rest ->
            if
              Cid.Set.mem cid missing
              || Block_map.has cid cache
              || Cid.Set.mem cid seen
            then collect acc seen remaining rest
            else
              collect (cid :: acc) (Cid.Set.add cid seen) (remaining - 1) rest
        | _ :: rest ->
            collect acc seen remaining rest
      in
      let cids, _seen = collect [] Cid.Set.empty batch_size queue in
      if List.is_empty cids then Lwt.return (cache, missing)
      else
        let%lwt bm = Store.get_blocks t.blockstore cids in
        let cache' =
          List.fold_left
            (fun acc (cid, bytes) -> Block_map.set cid bytes acc)
            cache (Block_map.entries bm.blocks)
        in
        let missing' =
          List.fold_left
            (fun acc cid -> Cid.Set.add cid acc)
            missing bm.missing
        in
        Lwt.return (cache', missing')
    in
    let rec step (queue, cache, missing) =
      match queue with
      | [] ->
          Lwt.return_none
      | `Leaf cid :: rest ->
          Lwt.return_some ((Leaf cid : ordered_item), (rest, cache, missing))
      | `Node cid :: rest ->
          if Cid.Set.mem cid missing then step (rest, cache, missing)
          else
            ( match Block_map.get cid cache with
            | None ->
                let%lwt cache', missing' = prefetch queue cache missing in
                if cache' == cache && Cid.Set.mem cid missing' then
                  step (rest, cache', missing')
                else step (queue, cache', missing')
            | Some bytes ->
                let node = decode_block_raw bytes in
                (* queue items: left subtree, then for each entry: record then right subtree *)
                let left_queue =
                  match node.l with Some l -> [`Node l] | None -> []
                in
                let entries_queue =
                  List.concat_map
                    (fun (e : entry_raw) ->
                      let right_queue =
                        match e.t with Some r -> [`Node r] | None -> []
                      in
                      `Leaf e.v :: right_queue )
                    node.e
                in
                let new_queue = left_queue @ entries_queue @ rest in
                let cache' = Block_map.remove cid cache in
                Lwt.return_some
                  ((Node (cid, bytes) : ordered_item), (new_queue, cache', missing))
            )
    in
    Lwt_seq.unfold_lwt step ([`Node t.root], Block_map.empty, Cid.Set.empty)

  (* produces a cid and cbor-encoded bytes for a given tree *)
  let serialize t node : (Cid.t * bytes, exn) Lwt_result.t =
    let rec aux node : (Cid.t * bytes) Lwt.t =
      let entries =
        if entries_are_sorted node.entries then node.entries
        else
          List.sort (fun (a : entry) b -> String.compare a.key b.key)
            node.entries
      in
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
                  Lwt.return_some cid
              | None ->
                  Lwt.return_none
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
          entries
      in
      let encoded =
        Dag_cbor.encode (encode_node_raw {l= left; e= mst_entries})
      in
      let cid = Cid.create Dcbor encoded in
      match%lwt Store.put_block t.blockstore cid encoded with
      | Ok _ ->
          Lwt.return (cid, encoded)
      | Error e ->
          raise e
    in
    try%lwt Lwt.map Result.ok (aux node)
    with e -> Lwt.return_error e

  (* raw-node helpers for covering proofs: operate on stored bytes, not re-serialization *)
  type interleaved_entry =
    | Tree of Cid.t
    | Leaf of string * Cid.t * Cid.t option

  (* returns a list of a node's entries' keys *)
  let node_entry_keys raw : string list =
    let last_key = ref "" in
    List.map
      (fun (e : entry_raw) ->
        let prefix = if e.p = 0 then "" else String.sub !last_key 0 e.p in
        let k = prefix ^ Bytes.to_string e.k in
        last_key := k ;
        k )
      raw.e

  (* returns a list of interleaved nodes & entries; needed for covering proofs *)
  let interleave_raw raw keys : interleaved_entry list =
    let start = match raw.l with Some l -> [Tree l] | None -> [] in
    let rec aux acc es ks =
      match (es, ks) with
      | [], [] ->
          acc
      | e :: etl, k :: ktl ->
          let acc' = Leaf (k, e.v, e.t) :: acc in
          let acc'' =
            match e.t with Some t -> Tree t :: acc' | None -> acc'
          in
          aux acc'' etl ktl
      | _ ->
          acc
    in
    aux start raw.e keys |> List.rev

  (* returns the index of the first leaf entry with a key greater than or equal to the given key *)
  let find_gte_leaf_index key entries : int =
    let rec aux i = function
      | [] ->
          i
      | Leaf (k, _, _) :: tl ->
          if k >= key then i else aux (i + 1) tl
      | Tree _ :: tl ->
          aux (i + 1) tl
    in
    aux 0 entries

  (* returns all mst nodes needed to prove the value of a given key *)
  let rec proof_for_key t cid key : Block_map.t Lwt.t =
    match%lwt Store.get_bytes t.blockstore cid with
    | None ->
        Lwt.return Block_map.empty
    | Some bytes ->
        let raw = decode_block_raw bytes in
        let keys = node_entry_keys raw in
        let seq = interleave_raw raw keys in
        let index = find_gte_leaf_index key seq in
        let%lwt blocks =
          match List.nth_opt seq index with
          | Some (Leaf (k, _, _)) when k = key ->
              Lwt.return Block_map.empty
          | Some (Leaf (_k, v_right, _)) -> (
              let prev =
                if index - 1 >= 0 then List.nth_opt seq (index - 1) else None
              in
              match prev with
              | Some (Tree c) ->
                  proof_for_key t c key
              | _ ->
                  (* include bounding neighbor leaf blocks to prove nonexistence *)
                  let left_leaf =
                    match prev with
                    | Some (Leaf (_, v_left, _)) ->
                        Some v_left
                    | _ ->
                        None
                  in
                  let%lwt bm =
                    match left_leaf with
                    | Some cid_left -> (
                      match%lwt Store.get_bytes t.blockstore cid_left with
                      | Some b ->
                          Lwt.return (Block_map.set cid_left b Block_map.empty)
                      | None ->
                          Lwt.return Block_map.empty )
                    | None ->
                        Lwt.return Block_map.empty
                  in
                  let%lwt bm =
                    match%lwt Store.get_bytes t.blockstore v_right with
                    | Some b ->
                        Lwt.return (Block_map.set v_right b bm)
                    | None ->
                        Lwt.return bm
                  in
                  Lwt.return bm )
          | Some (Tree c) ->
              proof_for_key t c key
          | None -> (
              let prev =
                if index - 1 >= 0 then List.nth_opt seq (index - 1) else None
              in
              match prev with
              | Some (Tree c) ->
                  proof_for_key t c key
              | _ ->
                  (* include bounding neighbor leaf blocks (if any) to prove nonexistence *)
                  let left_leaf =
                    match prev with
                    | Some (Leaf (_, v_left, _)) ->
                        Some v_left
                    | _ ->
                        None
                  in
                  let right_leaf =
                    match List.nth_opt seq index with
                    | Some (Leaf (_, v_right, _)) ->
                        Some v_right
                    | _ ->
                        None
                  in
                  let%lwt bm =
                    match left_leaf with
                    | Some cid_left -> (
                      match%lwt Store.get_bytes t.blockstore cid_left with
                      | Some b ->
                          Lwt.return (Block_map.set cid_left b Block_map.empty)
                      | None ->
                          Lwt.return Block_map.empty )
                    | None ->
                        Lwt.return Block_map.empty
                  in
                  let%lwt bm =
                    match right_leaf with
                    | Some cid_right -> (
                      match%lwt Store.get_bytes t.blockstore cid_right with
                      | Some b ->
                          Lwt.return (Block_map.set cid_right b bm)
                      | None ->
                          Lwt.return bm )
                    | None ->
                        Lwt.return bm
                  in
                  Lwt.return bm )
        in
        Lwt.return (Block_map.set cid bytes blocks)

  let proof_for_keys t cid keys : Block_map.t Lwt.t =
    if List.is_empty keys then Lwt.return Block_map.empty
    else
      let keys = List.sort_uniq String.compare keys in
      let cache = ref Block_map.empty in
      let missing = ref Cid.Set.empty in
      let acc = ref Block_map.empty in
      let add_block cid bytes =
        if not (Block_map.has cid !acc) then
          acc := Block_map.set cid bytes !acc
      in
      let get_bytes_cached cid =
        match Block_map.get cid !cache with
        | Some bytes ->
            Lwt.return_some bytes
        | None ->
            if Cid.Set.mem cid !missing then Lwt.return_none
            else
              let%lwt bytes_opt = Store.get_bytes t.blockstore cid in
              ( match bytes_opt with
              | Some bytes ->
                  cache := Block_map.set cid bytes !cache
              | None ->
                  missing := Cid.Set.add cid !missing ) ;
              Lwt.return bytes_opt
      in
      let add_leaf cid_opt =
        match cid_opt with
        | None ->
            Lwt.return_unit
        | Some leaf_cid -> (
            match%lwt get_bytes_cached leaf_cid with
            | Some bytes ->
                add_block leaf_cid bytes ;
                Lwt.return_unit
            | None ->
                Lwt.return_unit )
      in
      let rec proof_for_key_cached cid key =
        match%lwt get_bytes_cached cid with
        | None ->
            Lwt.return_unit
        | Some bytes ->
            add_block cid bytes ;
            let raw = decode_block_raw bytes in
            let keys = node_entry_keys raw in
            let seq = interleave_raw raw keys in
            let index = find_gte_leaf_index key seq in
            ( match List.nth_opt seq index with
            | Some (Leaf (k, _, _)) when k = key ->
                Lwt.return_unit
            | Some (Leaf (_k, v_right, _)) -> (
                let prev =
                  if index - 1 >= 0 then List.nth_opt seq (index - 1) else None
                in
                match prev with
                | Some (Tree c) ->
                    proof_for_key_cached c key
                | _ ->
                    let left_leaf =
                      match prev with
                      | Some (Leaf (_, v_left, _)) ->
                          Some v_left
                      | _ ->
                          None
                    in
                    let%lwt () = add_leaf left_leaf in
                    add_leaf (Some v_right) )
            | Some (Tree c) ->
                proof_for_key_cached c key
            | None -> (
                let prev =
                  if index - 1 >= 0 then List.nth_opt seq (index - 1) else None
                in
                match prev with
                | Some (Tree c) ->
                    proof_for_key_cached c key
                | _ ->
                    let left_leaf =
                      match prev with
                      | Some (Leaf (_, v_left, _)) ->
                          Some v_left
                      | _ ->
                          None
                    in
                    let right_leaf =
                      match List.nth_opt seq index with
                      | Some (Leaf (_, v_right, _)) ->
                          Some v_right
                      | _ ->
                          None
                    in
                    let%lwt () = add_leaf left_leaf in
                    add_leaf right_leaf ) )
      in
      let%lwt () = Lwt_list.iter_s (proof_for_key_cached cid) keys in
      Lwt.return !acc

  (* collects all node blocks (cid, bytes) and all leaf cids reachable from root
     only traverses nodes; doesn't fetch leaf blocks
     returns (nodes, visited, leaves) *)
  let collect_nodes_and_leaves t :
      ((Cid.t * bytes) list * Cid.Set.t * Cid.Set.t) Lwt.t =
    let rec loop queue visited nodes leaves =
      match queue with
      | [] ->
          Lwt.return (nodes, visited, leaves)
      | _ ->
          let batch, rest = take_n batch_size queue in
          let to_fetch =
            List.filter (fun cid -> not (Cid.Set.mem cid visited)) batch
          in
          let%lwt blocks = get_blocks_exn t to_fetch in
          let visited', nodes', leaves', next_queue =
            List.fold_left
              (fun (visited, nodes, leaves, queue) cid ->
                if Cid.Set.mem cid visited then (visited, nodes, leaves, queue)
                else
                  let bytes = Block_map.get cid blocks |> Option.get in
                  let raw = decode_block_raw bytes in
                  let next_cids =
                    let acc = match raw.l with Some l -> [l] | None -> [] in
                    List.fold_left
                      (fun acc e ->
                        match e.t with Some r -> r :: acc | None -> acc )
                      acc raw.e
                  in
                  let leaves' =
                    List.fold_left (fun s e -> Cid.Set.add e.v s) leaves raw.e
                  in
                  let visited' = Cid.Set.add cid visited in
                  ( visited'
                  , (cid, bytes) :: nodes
                  , leaves'
                  , List.rev_append next_cids queue ) )
              (visited, nodes, leaves, rest) batch
          in
          loop next_queue visited' nodes' leaves'
    in
    loop [t.root] Cid.Set.empty [] Cid.Set.empty

  (* list of all leaves belonging to a node and its children, ordered by key *)
  let rec leaves_of_node n : (string * Cid.t) list Lwt.t =
    let%lwt left_leaves =
      n.left >>? function Some l -> leaves_of_node l | None -> Lwt.return []
    in
    let entries =
      if entries_are_sorted n.entries then n.entries
      else
        List.sort
          (fun (a : entry) (b : entry) -> String.compare a.key b.key)
          n.entries
    in
    let%lwt entry_sublists =
      Lwt_list.map_s
        (fun e ->
          let%lwt right_leaves =
            e.right
            >>? function Some r -> leaves_of_node r | None -> Lwt.return []
          in
          Lwt.return ((e.key, e.value) :: right_leaves) )
        entries
    in
    Lwt.return (left_leaves @ List.concat entry_sublists)

  (* list of all leaves in the mst *)
  let leaves_of_root t : (string * Cid.t) list Lwt.t =
    match%lwt retrieve_node t t.root with
    | None ->
        failwith "root cid not found in repo store"
    | Some root ->
        leaves_of_node root

  (* returns a count of all leaves in the mst *)
  let leaf_count t : int Lwt.t =
    let rec loop queue visited acc =
      match queue with
      | [] ->
          Lwt.return acc
      | _ ->
          let batch, rest = take_n batch_size queue in
          let to_fetch =
            List.filter (fun cid -> not (Cid.Set.mem cid visited)) batch
          in
          let%lwt blocks = get_blocks_exn t to_fetch in
          let visited', acc', next_queue =
            List.fold_left
              (fun (visited, acc, queue) cid ->
                if Cid.Set.mem cid visited then (visited, acc, queue)
                else
                  let bytes = Block_map.get cid blocks |> Option.get in
                  let raw = decode_block_raw bytes in
                  let next_cids =
                    let acc = match raw.l with Some l -> [l] | None -> [] in
                    List.fold_left
                      (fun acc e ->
                        match e.t with Some r -> r :: acc | None -> acc )
                      acc raw.e
                  in
                  let visited' = Cid.Set.add cid visited in
                  ( visited'
                  , acc + List.length raw.e
                  , List.rev_append next_cids queue ) )
              (visited, acc, rest) batch
          in
          loop next_queue visited' acc'
    in
    loop [t.root] Cid.Set.empty 0

  (* returns height of mst root *)
  let layer t : int Lwt.t =
    match%lwt retrieve_node t t.root with
    | None ->
        failwith "root cid not found in repo store"
    | Some root ->
        Lwt.return root.layer

  (* returns all nodes sorted by cid *)
  let all_nodes t : (Cid.t * bytes) list Lwt.t =
    let rec loop queue visited nodes =
      match queue with
      | [] ->
          Lwt.return nodes
      | _ ->
          let batch, rest = take_n batch_size queue in
          let to_fetch =
            List.filter (fun cid -> not (Cid.Set.mem cid visited)) batch
          in
          let%lwt blocks = get_blocks_exn t to_fetch in
          let visited', nodes', next_queue =
            List.fold_left
              (fun (visited, nodes, queue) cid ->
                if Cid.Set.mem cid visited then (visited, nodes, queue)
                else
                  let bytes = Block_map.get cid blocks |> Option.get in
                  let raw = decode_block_raw bytes in
                  let next_cids =
                    let acc = match raw.l with Some l -> [l] | None -> [] in
                    List.fold_left
                      (fun acc e ->
                        match e.t with Some r -> r :: acc | None -> acc )
                      acc raw.e
                  in
                  let visited' = Cid.Set.add cid visited in
                  ( visited'
                  , (cid, bytes) :: nodes
                  , List.rev_append next_cids queue ) )
              (visited, nodes, rest) batch
          in
          loop next_queue visited' nodes'
    in
    let%lwt nodes = loop [t.root] Cid.Set.empty [] in
    let sorted =
      List.sort
        (fun (a, _) (b, _) -> String.compare (Cid.to_string a) (Cid.to_string b))
        nodes
    in
    Lwt.return sorted

  (* creates and persists an empty mst *)
  let create_empty blockstore : (t, exn) Lwt_result.t =
    let encoded = Dag_cbor.encode (encode_node_raw {l= None; e= []}) in
    let cid = Cid.create Dcbor encoded in
    Lwt_result.bind (Store.put_block blockstore cid encoded) (fun _ ->
        Lwt.return_ok {blockstore; root= cid} )

  (* helper to propagate put_block errors *)
  let put_block_exn blockstore cid encoded =
    match%lwt Store.put_block blockstore cid encoded with
    | Ok _ ->
        Lwt.return_unit
    | Error e ->
        raise e

  (* builds and persists a canonical mst from sorted leaves *)
  let of_assoc blockstore assoc : t Lwt.t =
    let sorted =
      List.sort (fun (k1, _) (k2, _) -> String.compare k1 k2) assoc
    in
    List.iter (fun (k, _) -> Util.ensure_valid_key k) sorted ;
    (* persist_from_sorted returns (cid, layer) for the subtree it creates *)
    let rec persist_from_sorted (pairs : (string * Cid.t) list) :
        (Cid.t * int) Lwt.t =
      match pairs with
      | [] ->
          let encoded = Dag_cbor.encode (encode_node_raw {l= None; e= []}) in
          let cid = Cid.create Dcbor encoded in
          let%lwt () = put_block_exn blockstore cid encoded in
          Lwt.return (cid, 0)
      | _ ->
          let with_layers =
            List.map (fun (k, v) -> (k, v, Util.leading_zeros_on_hash k)) pairs
          in
          let root_layer =
            List.fold_left (fun acc (_, _, lz) -> max acc lz) 0 with_layers
          in
          let left_group, on_layer, right_groups =
            let left_group = ref [] in
            let current_group = ref [] in
            let on_layer_rev = ref [] in
            let groups_rev = ref [] in
            let seen_on = ref false in
            List.iter
              (fun (k, v, lz) ->
                if lz = root_layer then (
                  if not !seen_on then left_group := List.rev !current_group
                  else groups_rev := List.rev !current_group :: !groups_rev ;
                  current_group := [] ;
                  on_layer_rev := (k, v) :: !on_layer_rev ;
                  seen_on := true )
                else current_group := (k, v) :: !current_group )
              with_layers ;
            let on_layer = List.rev !on_layer_rev in
            let right_groups =
              if not !seen_on then []
              else List.rev (List.rev !current_group :: !groups_rev)
            in
            (!left_group, on_layer, right_groups)
          in
          let%lwt l_cid =
            match left_group with
            | [] ->
                Lwt.return_none
            | lst ->
                (* persist left subtree and wrap up to root_layer - 1 *)
                let%lwt cid, child_layer = persist_from_sorted lst in
                let rec wrap cid layer =
                  if layer >= root_layer - 1 then Lwt.return cid
                  else
                    let encoded =
                      Dag_cbor.encode (encode_node_raw {l= Some cid; e= []})
                    in
                    let cid' = Cid.create Dcbor encoded in
                    let%lwt () = put_block_exn blockstore cid' encoded in
                    wrap cid' (layer + 1)
                in
                let%lwt c = wrap cid child_layer in
                Lwt.return_some c
          in
          let rights =
            List.map2 (fun (k, _) grp -> (k, grp)) on_layer right_groups
          in
          let%lwt t_links =
            Lwt_list.map_s
              (fun (_k, grp) ->
                match grp with
                | [] ->
                    Lwt.return_none
                | lst ->
                    (* persist child subtree and wrap up to root_layer - 1 *)
                    let%lwt cid, child_layer = persist_from_sorted lst in
                    let rec wrap cid layer =
                      if layer >= root_layer - 1 then Lwt.return cid
                      else
                        let encoded =
                          Dag_cbor.encode (encode_node_raw {l= Some cid; e= []})
                        in
                        let cid' = Cid.create Dcbor encoded in
                        let%lwt () = put_block_exn blockstore cid' encoded in
                        wrap cid' (layer + 1)
                    in
                    let%lwt c = wrap cid child_layer in
                    Lwt.return_some c )
              rights
          in
          let entries_raw =
            let last_key = ref "" in
            List.mapi
              (fun i (k, v) ->
                let p = Util.shared_prefix_length !last_key k in
                let k_suffix =
                  String.sub k p (String.length k - p) |> Bytes.of_string
                in
                last_key := k ;
                let t = List.nth t_links i in
                ({p; k= k_suffix; v; t} : entry_raw) )
              on_layer
          in
          let node_raw = {l= l_cid; e= entries_raw} in
          let encoded = Dag_cbor.encode (encode_node_raw node_raw) in
          let cid = Cid.create Dcbor encoded in
          let%lwt () = put_block_exn blockstore cid encoded in
          Lwt.return (cid, root_layer)
    in
    let%lwt root, _ = persist_from_sorted sorted in
    Lwt.return {blockstore; root}

  (* insert or replace an entry, constructing a new canonical mst from scratch *)
  let add_rebuild t key cid : t Lwt.t =
    Util.ensure_valid_key key ;
    let%lwt leaves = leaves_of_root t in
    let without = List.filter (fun (k, _) -> k <> key) leaves in
    of_assoc t.blockstore ((key, cid) :: without)

  (* delete an entry, constructing a new canonical mst from scratch *)
  let delete_rebuild t key : t Lwt.t =
    Util.ensure_valid_key key ;
    let%lwt leaves = leaves_of_root t in
    let remaining = List.filter (fun (k, _) -> k <> key) leaves in
    of_assoc t.blockstore remaining

  (* persist a raw node and return its cid *)
  let persist_node_raw (blockstore : bs) (raw : node_raw) : Cid.t Lwt.t =
    let encoded = Dag_cbor.encode (encode_node_raw raw) in
    let cid = Cid.create Dcbor encoded in
    let%lwt () = put_block_exn blockstore cid encoded in
    Lwt.return cid

  (* decompress entry keys from a raw node *)
  let decompress_keys (raw : node_raw) : string list =
    let last_key = ref "" in
    List.map
      (fun (e : entry_raw) ->
        let prefix = if e.p = 0 then "" else String.sub !last_key 0 e.p in
        let k = prefix ^ Bytes.to_string e.k in
        last_key := k ;
        k )
      raw.e

  (* compress a list of (key, value, right) tuples into an entry_raw list *)
  let compress_entries (entries : (string * Cid.t * Cid.t option) list) :
      entry_raw list =
    let last_key = ref "" in
    List.map
      (fun (k, v, t) ->
        let p = Util.shared_prefix_length !last_key k in
        let k_suffix =
          String.sub k p (String.length k - p) |> Bytes.of_string
        in
        last_key := k ;
        {p; k= k_suffix; v; t} )
      entries

  (* wrap a subtree cid up to target layer with empty intermediate nodes *)
  let rec wrap_to_layer (blockstore : bs) (cid : Cid.t) (from_layer : int)
      (to_layer : int) : Cid.t Lwt.t =
    if from_layer >= to_layer then Lwt.return cid
    else
      let raw = {l= Some cid; e= []} in
      let%lwt wrapped_cid = persist_node_raw blockstore raw in
      wrap_to_layer blockstore wrapped_cid (from_layer + 1) to_layer

  (* collect all leaves from a subtree *)
  let collect_subtree_leaves (t : t) (cid : Cid.t) : (string * Cid.t) list Lwt.t
      =
    match%lwt retrieve_node t cid with
    | Some node ->
        leaves_of_node node
    | None ->
        Lwt.return []

  (* rebuild a subtree from leaves
     returns (root_cid option, actual_layer) *)
  let rebuild_subtree (blockstore : bs) (leaves : (string * Cid.t) list) :
      (Cid.t option * int) Lwt.t =
    match leaves with
    | [] ->
        Lwt.return (None, 0)
    | _ -> (
        let%lwt result = of_assoc blockstore leaves in
        let t' = {blockstore; root= result.root} in
        match%lwt retrieve_node_raw t' result.root with
        | Some raw ->
            let%lwt layer = get_node_height t' raw in
            Lwt.return (Some result.root, layer)
        | None ->
            Lwt.return (Some result.root, 0) )

  (* find the position where a key would be located in a node's entries *)
  type position =
    | PLeft
    | PEntry of int (* key matches entry at index *)
    | PRight of int (* key belongs in right subtree of entry at index *)

  let find_position (keys : string list) (key : string) : position =
    let rec aux i = function
      | [] -> (
        match i with 0 -> PLeft | _ -> PRight (i - 1) )
      | k :: rest ->
          if key = k then PEntry i
          else if key < k then if i = 0 then PLeft else PRight (i - 1)
          else aux (i + 1) rest
    in
    aux 0 keys

  let rec add_incremental_raw (t : t) (root_cid : Cid.t) (key : string)
      (value : Cid.t) (key_layer : int) : Cid.t Lwt.t =
    match%lwt retrieve_node_raw t root_cid with
    | None ->
        failwith ("couldn't find node " ^ Cid.to_string root_cid)
    | Some raw ->
        let%lwt root_layer = get_node_height t raw in
        if key_layer > root_layer then
          add_above_root t root_cid root_layer key value key_layer
        else if key_layer = root_layer then
          add_at_level t raw root_layer key value
        else add_below_level t raw root_layer key value key_layer

  and add_above_root (t : t) (old_root_cid : Cid.t) (old_root_layer : int)
      (key : string) (value : Cid.t) (key_layer : int) : Cid.t Lwt.t =
    (* wrap old root up to key_layer - 1 *)
    let%lwt wrapped_old =
      wrap_to_layer t.blockstore old_root_cid old_root_layer (key_layer - 1)
    in
    (* get all keys from old tree to determine position *)
    let%lwt old_leaves = collect_subtree_leaves t old_root_cid in
    let old_keys = List.map fst old_leaves in
    let all_less = List.for_all (fun k -> k < key) old_keys in
    let all_greater = List.for_all (fun k -> k > key) old_keys in
    if all_less then
      (* all old keys < new key: old tree is left, new entry has no right *)
      let entries = compress_entries [(key, value, None)] in
      persist_node_raw t.blockstore {l= Some wrapped_old; e= entries}
    else if all_greater then
      (* all old keys > new key: new entry first with old tree as right *)
      let entries = compress_entries [(key, value, Some wrapped_old)] in
      persist_node_raw t.blockstore {l= None; e= entries}
    else
      (* key is in the middle: need to split *)
      let left_leaves = List.filter (fun (k, _) -> k < key) old_leaves in
      let right_leaves = List.filter (fun (k, _) -> k > key) old_leaves in
      let%lwt left_cid, left_layer = rebuild_subtree t.blockstore left_leaves in
      let%lwt left_wrapped =
        match left_cid with
        | Some cid ->
            let%lwt wrapped =
              wrap_to_layer t.blockstore cid left_layer (key_layer - 1)
            in
            Lwt.return_some wrapped
        | None ->
            Lwt.return_none
      in
      let%lwt right_cid, right_layer =
        rebuild_subtree t.blockstore right_leaves
      in
      let%lwt right_wrapped =
        match right_cid with
        | Some cid ->
            let%lwt wrapped =
              wrap_to_layer t.blockstore cid right_layer (key_layer - 1)
            in
            Lwt.return_some wrapped
        | None ->
            Lwt.return_none
      in
      let entries = compress_entries [(key, value, right_wrapped)] in
      persist_node_raw t.blockstore {l= left_wrapped; e= entries}

  and add_at_level (t : t) (raw : node_raw) (layer : int) (key : string)
      (value : Cid.t) : Cid.t Lwt.t =
    let keys = decompress_keys raw in
    let pos = find_position keys key in
    match pos with
    | PEntry i ->
        (* key already exists, just update value *)
        let entries =
          List.mapi (fun j e -> if j = i then {e with v= value} else e) raw.e
        in
        persist_node_raw t.blockstore {raw with e= entries}
    | PLeft ->
        (* key goes before all entries, but at same layer it becomes an entry *)
        (* need to check if there's a left subtree to handle *)
        let%lwt new_left, new_right =
          match raw.l with
          | None ->
              Lwt.return (None, None)
          | Some left_cid ->
              (* collect leaves from left subtree and split around the new key *)
              let%lwt left_leaves = collect_subtree_leaves t left_cid in
              let new_left_leaves =
                List.filter (fun (k, _) -> k < key) left_leaves
              in
              let new_right_leaves =
                List.filter (fun (k, _) -> k > key) left_leaves
              in
              let%lwt new_left_cid, new_left_layer =
                rebuild_subtree t.blockstore new_left_leaves
              in
              let%lwt new_left_wrapped =
                match new_left_cid with
                | Some cid ->
                    let%lwt w =
                      wrap_to_layer t.blockstore cid new_left_layer (layer - 1)
                    in
                    Lwt.return_some w
                | None ->
                    Lwt.return_none
              in
              let%lwt new_right_cid, new_right_layer =
                rebuild_subtree t.blockstore new_right_leaves
              in
              let%lwt new_right_wrapped =
                match new_right_cid with
                | Some cid ->
                    let%lwt w =
                      wrap_to_layer t.blockstore cid new_right_layer (layer - 1)
                    in
                    Lwt.return_some w
                | None ->
                    Lwt.return_none
              in
              Lwt.return (new_left_wrapped, new_right_wrapped)
        in
        let old_entries = List.map2 (fun k e -> (k, e.v, e.t)) keys raw.e in
        let first_entry = (key, value, new_right) in
        let new_entries = compress_entries (first_entry :: old_entries) in
        persist_node_raw t.blockstore {l= new_left; e= new_entries}
    | PRight i ->
        (* key goes after entry i, before entry i+1 *)
        let entry_i = List.nth raw.e i in
        let%lwt new_right_for_i, new_right_for_key =
          match entry_i.t with
          | None ->
              Lwt.return (None, None)
          | Some right_cid ->
              let%lwt right_leaves = collect_subtree_leaves t right_cid in
              let stays_right =
                List.filter (fun (k, _) -> k < key) right_leaves
              in
              let goes_to_new =
                List.filter (fun (k, _) -> k > key) right_leaves
              in
              let%lwt stays_cid, stays_layer =
                rebuild_subtree t.blockstore stays_right
              in
              let%lwt stays_wrapped =
                match stays_cid with
                | Some cid ->
                    let%lwt w =
                      wrap_to_layer t.blockstore cid stays_layer (layer - 1)
                    in
                    Lwt.return_some w
                | None ->
                    Lwt.return_none
              in
              let%lwt goes_cid, goes_layer =
                rebuild_subtree t.blockstore goes_to_new
              in
              let%lwt goes_wrapped =
                match goes_cid with
                | Some cid ->
                    let%lwt w =
                      wrap_to_layer t.blockstore cid goes_layer (layer - 1)
                    in
                    Lwt.return_some w
                | None ->
                    Lwt.return_none
              in
              Lwt.return (stays_wrapped, goes_wrapped)
        in
        let old_entries = List.map2 (fun k e -> (k, e.v, e.t)) keys raw.e in
        let new_entries =
          List.mapi
            (fun j (k, v, r) ->
              if j = i then (k, v, new_right_for_i)
              else if j = i + 1 then (k, v, r)
              else (k, v, r) )
            old_entries
        in
        let rec insert_after idx acc = function
          | [] ->
              List.rev ((key, value, new_right_for_key) :: acc)
          | hd :: tl ->
              if idx = i then
                List.rev_append acc (hd :: (key, value, new_right_for_key) :: tl)
              else insert_after (idx + 1) (hd :: acc) tl
        in
        let final_entries = insert_after 0 [] new_entries in
        let compressed = compress_entries final_entries in
        persist_node_raw t.blockstore {raw with e= compressed}

  and add_below_level (t : t) (raw : node_raw) (root_layer : int) (key : string)
      (value : Cid.t) (key_layer : int) : Cid.t Lwt.t =
    let keys = decompress_keys raw in
    let pos = find_position keys key in
    match pos with
    | PEntry i ->
        let entries =
          List.mapi (fun j e -> if j = i then {e with v= value} else e) raw.e
        in
        persist_node_raw t.blockstore {raw with e= entries}
    | PLeft -> (
      match raw.l with
      | None ->
          (* create a left subtree with just this entry *)
          let%lwt new_left_cid, new_left_layer =
            rebuild_subtree t.blockstore [(key, value)]
          in
          let%lwt new_left =
            match new_left_cid with
            | Some cid ->
                let%lwt w =
                  wrap_to_layer t.blockstore cid new_left_layer (root_layer - 1)
                in
                Lwt.return_some w
            | None ->
                Lwt.return_none
          in
          persist_node_raw t.blockstore {raw with l= new_left}
      | Some left_cid ->
          (* recurse into left subtree *)
          let%lwt new_left_cid =
            add_incremental_raw t left_cid key value key_layer
          in
          (* check if tree grew *)
          let%lwt new_left_raw_opt = retrieve_node_raw t new_left_cid in
          let%lwt new_left_layer =
            match new_left_raw_opt with
            | Some r ->
                get_node_height t r
            | None ->
                Lwt.return 0
          in
          if new_left_layer >= root_layer then
            (* left subtree grew to our level or above, need to merge *)
            let%lwt left_leaves = collect_subtree_leaves t new_left_cid in
            let%lwt my_entries =
              Lwt_list.map_s
                (fun (k, e) ->
                  let%lwt right_leaves =
                    match e.t with
                    | Some cid ->
                        collect_subtree_leaves t cid
                    | None ->
                        Lwt.return []
                  in
                  Lwt.return ((k, e.v) :: right_leaves) )
                (List.combine keys raw.e)
            in
            let all_leaves = left_leaves @ List.concat my_entries in
            let%lwt result = of_assoc t.blockstore all_leaves in
            Lwt.return result.root
          else
            (* wrap if needed and update *)
            let%lwt wrapped =
              wrap_to_layer t.blockstore new_left_cid new_left_layer
                (root_layer - 1)
            in
            persist_node_raw t.blockstore {raw with l= Some wrapped} )
    | PRight i -> (
        let entry_i = List.nth raw.e i in
        match entry_i.t with
        | None ->
            (* create a right subtree with just this entry *)
            let%lwt new_right_cid, new_right_layer =
              rebuild_subtree t.blockstore [(key, value)]
            in
            let%lwt new_right =
              match new_right_cid with
              | Some cid ->
                  let%lwt w =
                    wrap_to_layer t.blockstore cid new_right_layer
                      (root_layer - 1)
                  in
                  Lwt.return_some w
              | None ->
                  Lwt.return_none
            in
            let entries =
              List.mapi
                (fun j e -> if j = i then {e with t= new_right} else e)
                raw.e
            in
            persist_node_raw t.blockstore {raw with e= entries}
        | Some right_cid ->
            (* recurse into right subtree *)
            let%lwt new_right_cid =
              add_incremental_raw t right_cid key value key_layer
            in
            (* check if layer changed *)
            let%lwt new_right_raw_opt = retrieve_node_raw t new_right_cid in
            let%lwt new_right_layer =
              match new_right_raw_opt with
              | Some r ->
                  get_node_height t r
              | None ->
                  Lwt.return 0
            in
            if new_right_layer >= root_layer then
              let%lwt pre_leaves =
                match raw.l with
                | Some cid ->
                    collect_subtree_leaves t cid
                | None ->
                    Lwt.return []
              in
              let%lwt entry_leaves =
                Lwt_list.mapi_s
                  (fun j (k, e) ->
                    let%lwt right_leaves =
                      if j = i then collect_subtree_leaves t new_right_cid
                      else
                        match e.t with
                        | Some cid ->
                            collect_subtree_leaves t cid
                        | None ->
                            Lwt.return []
                    in
                    Lwt.return ((k, e.v) :: right_leaves) )
                  (List.combine keys raw.e)
              in
              let all_leaves = pre_leaves @ List.concat entry_leaves in
              let%lwt result = of_assoc t.blockstore all_leaves in
              Lwt.return result.root
            else
              (* wrap if needed and update *)
              let%lwt wrapped =
                wrap_to_layer t.blockstore new_right_cid new_right_layer
                  (root_layer - 1)
              in
              let entries =
                List.mapi
                  (fun j e -> if j = i then {e with t= Some wrapped} else e)
                  raw.e
              in
              persist_node_raw t.blockstore {raw with e= entries} )

  (* insert or replace an entry *)
  let add (t : t) (key : string) (value : Cid.t) : t Lwt.t =
    Util.ensure_valid_key key ;
    let key_layer = Util.leading_zeros_on_hash key in
    match%lwt retrieve_node_raw t t.root with
    | None ->
        failwith "root cid not found"
    | Some raw when raw.l = None && raw.e = [] ->
        let entries = compress_entries [(key, value, None)] in
        let%lwt new_root =
          persist_node_raw t.blockstore {l= None; e= entries}
        in
        Lwt.return {t with root= new_root}
    | Some _ ->
        let%lwt new_root = add_incremental_raw t t.root key value key_layer in
        Lwt.return {t with root= new_root}

  (* delete an entry
     returns (new_cid option, layer) where layer is the layer of the returned subtree
     None means the subtree is now empty *)
  let rec delete_incremental_raw (t : t) (root_cid : Cid.t) (key : string)
      (key_layer : int) : (Cid.t * int) option Lwt.t =
    match%lwt retrieve_node_raw t root_cid with
    | None ->
        Lwt.return_none
    | Some raw ->
        let%lwt root_layer = get_node_height t raw in
        if key_layer > root_layer then
          (* key can't exist above root *)
          Lwt.return_some (root_cid, root_layer)
        else if key_layer = root_layer then
          delete_at_level t raw root_cid root_layer key
        else delete_below_level t raw root_cid root_layer key key_layer

  (* delete entry at current level
     returns (cid, layer) or None if empty *)
  and delete_at_level (t : t) (raw : node_raw) (node_cid : Cid.t) (layer : int)
      (key : string) : (Cid.t * int) option Lwt.t =
    let keys = decompress_keys raw in
    let pos = find_position keys key in
    match pos with
    | PEntry i ->
        let entry_i = List.nth raw.e i in
        (* merge adjacent subtrees *)
        let%lwt left_leaves =
          if i = 0 then
            match raw.l with
            | Some cid ->
                collect_subtree_leaves t cid
            | None ->
                Lwt.return []
          else
            let prev_entry = List.nth raw.e (i - 1) in
            match prev_entry.t with
            | Some cid ->
                collect_subtree_leaves t cid
            | None ->
                Lwt.return []
        in
        let%lwt right_leaves =
          match entry_i.t with
          | Some cid ->
              collect_subtree_leaves t cid
          | None ->
              Lwt.return []
        in
        let merged_leaves = left_leaves @ right_leaves in
        let remaining_entries =
          List.filteri (fun j _ -> j <> i) (List.combine keys raw.e)
        in
        if remaining_entries = [] then
          match merged_leaves with
          | [] ->
              Lwt.return_none
          | _ ->
              let%lwt result = of_assoc t.blockstore merged_leaves in
              let%lwt result_layer =
                match%lwt retrieve_node_raw t result.root with
                | Some r ->
                    get_node_height t r
                | None ->
                    Lwt.return 0
              in
              Lwt.return_some (result.root, result_layer)
        else
          let%lwt merged_cid_opt, merged_layer =
            rebuild_subtree t.blockstore merged_leaves
          in
          let%lwt merged_wrapped =
            match merged_cid_opt with
            | Some cid ->
                let%lwt w =
                  wrap_to_layer t.blockstore cid merged_layer (layer - 1)
                in
                Lwt.return_some w
            | None ->
                Lwt.return_none
          in
          let new_left, entries_tuples =
            if i = 0 then
              let entries =
                List.map (fun (k, e) -> (k, e.v, e.t)) remaining_entries
              in
              (merged_wrapped, entries)
            else
              let entries =
                List.mapi
                  (fun j (k, e) ->
                    if j = i - 1 then (k, e.v, merged_wrapped) else (k, e.v, e.t) )
                  remaining_entries
              in
              (raw.l, entries)
          in
          let compressed = compress_entries entries_tuples in
          let%lwt new_cid =
            persist_node_raw t.blockstore {l= new_left; e= compressed}
          in
          Lwt.return_some (new_cid, layer)
    | PLeft | PRight _ ->
        (* key not at this level -> doesn't exist *)
        Lwt.return_some (node_cid, layer)

  (* delete in a subtree, wrapping if subtree layer changes *)
  and delete_below_level (t : t) (raw : node_raw) (root_cid : Cid.t)
      (root_layer : int) (key : string) (key_layer : int) :
      (Cid.t * int) option Lwt.t =
    let keys = decompress_keys raw in
    let pos = find_position keys key in
    match pos with
    | PEntry _ ->
        (* key layer < root layer -> key doesn't exist *)
        Lwt.return_some (root_cid, root_layer)
    | PLeft -> (
      match raw.l with
      | None ->
          (* key doesn't exist *)
          Lwt.return_some (root_cid, root_layer)
      | Some left_cid -> (
          let%lwt result = delete_incremental_raw t left_cid key key_layer in
          match result with
          | None ->
              (* subtree became empty *)
              let%lwt cid = persist_node_raw t.blockstore {raw with l= None} in
              Lwt.return_some (cid, root_layer)
          | Some (new_left_cid, new_left_layer) ->
              if Cid.equal new_left_cid left_cid then
                Lwt.return_some (root_cid, root_layer)
              else
                (* wrap if layer changed *)
                let%lwt wrapped =
                  wrap_to_layer t.blockstore new_left_cid new_left_layer
                    (root_layer - 1)
                in
                let%lwt cid =
                  persist_node_raw t.blockstore {raw with l= Some wrapped}
                in
                Lwt.return_some (cid, root_layer) ) )
    | PRight i -> (
        let entry_i = List.nth raw.e i in
        match entry_i.t with
        | None ->
            (* no right subtree -> key doesn't exist *)
            Lwt.return_some (root_cid, root_layer)
        | Some right_cid -> (
            let%lwt result = delete_incremental_raw t right_cid key key_layer in
            match result with
            | None ->
                (* subtree became empty *)
                let entries =
                  List.mapi
                    (fun j e -> if j = i then {e with t= None} else e)
                    raw.e
                in
                let%lwt cid =
                  persist_node_raw t.blockstore {raw with e= entries}
                in
                Lwt.return_some (cid, root_layer)
            | Some (new_right_cid, new_right_layer) ->
                if Cid.equal new_right_cid right_cid then
                  Lwt.return_some (root_cid, root_layer)
                else
                  (* wrap if layer changed *)
                  let%lwt wrapped =
                    wrap_to_layer t.blockstore new_right_cid new_right_layer
                      (root_layer - 1)
                  in
                  let entries =
                    List.mapi
                      (fun j e -> if j = i then {e with t= Some wrapped} else e)
                      raw.e
                  in
                  let%lwt cid =
                    persist_node_raw t.blockstore {raw with e= entries}
                  in
                  Lwt.return_some (cid, root_layer) ) )

  (* delete an entry *)
  let delete (t : t) (key : string) : t Lwt.t =
    Util.ensure_valid_key key ;
    let key_layer = Util.leading_zeros_on_hash key in
    match%lwt delete_incremental_raw t t.root key key_layer with
    | None -> (
      (* Tree became empty *)
      match%lwt
        create_empty t.blockstore
      with
      | Ok empty ->
          Lwt.return empty
      | Error e ->
          raise e )
    | Some (new_root, _layer) ->
        Lwt.return {t with root= new_root}

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
