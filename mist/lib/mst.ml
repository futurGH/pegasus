open Storage
module String_map = Dag_cbor.StringMap

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

type node =
  { layer: int
  ; mutable left: node option Lwt.t Lazy.t
  ; mutable entries: entry list }

and entry =
  {layer: int; key: string; value: Cid.t; right: node option Lwt.t Lazy.t}

type node_or_entry = Node of node | Entry of entry

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

let ( let*? ) lazy_opt_lwt f =
  let%lwt result = Lazy.force lazy_opt_lwt in
  f result

let ( >>? ) lazy_opt_lwt f =
  let%lwt result = Lazy.force lazy_opt_lwt in
  f result

module type Intf = sig
  module Store : Writable_blockstore

  type t = {blockstore: Store.t; root: Cid.t}

  val create : Store.t -> Cid.t -> t

  val retrieve_node_raw : t -> Cid.t -> node_raw option Lwt.t

  val retrieve_node : t -> Cid.t -> node option Lwt.t

  val retrieve_node_lazy : t -> Cid.t -> node option Lwt.t lazy_t

  val get_node_height : t -> node_raw -> int Lwt.t

  val traverse : t -> (string -> Cid.t -> unit) -> unit Lwt.t

  val build_map : t -> Cid.t String_map.t Lwt.t

  val to_blocks_stream : t -> (Cid.t * bytes) Lwt_seq.t

  val serialize : t -> node -> (Cid.t * bytes, exn) Lwt_result.t

  val proof_for_key : t -> Cid.t -> string -> Block_map.t Lwt.t

  val get_covering_proof : t -> string -> Storage.Block_map.t Lwt.t

  val leaf_count : t -> int Lwt.t

  val layer : t -> int Lwt.t

  val all_nodes : t -> (Cid.t * bytes) list Lwt.t

  val create_empty : Store.t -> (t, exn) Lwt_result.t

  val get_cid : t -> string -> Cid.t option Lwt.t

  val of_assoc : Store.t -> (string * Cid.t) list -> t Lwt.t

  val add : t -> string -> Cid.t -> t Lwt.t

  val delete : t -> string -> t Lwt.t

  val collect_nodes_and_leaves :
    t -> ((Cid.t * bytes) list * Cid.Set.t * Cid.Set.t) Lwt.t

  val leaves_of_node : node -> (string * Cid.t) list Lwt.t

  val leaves_of_root : t -> (string * Cid.t) list Lwt.t

  val null_diff : t -> data_diff Lwt.t

  val equal : t -> t -> bool Lwt.t
end

module Make (Store : Writable_blockstore) : Intf with module Store = Store =
struct
  module Store = Store

  type bs = Store.t

  type t = {blockstore: bs; root: Cid.t}

  let create blockstore root = {blockstore; root}

  (* decodes a node retrieved from the blockstore *)
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
        (fun (entries : entry list) entry ->
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
          ({layer; key= path; value= entry.v; right} : entry) :: entries )
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
      List.iter (fun (entry : entry) -> fn entry.key entry.value) node.entries ;
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
    let%lwt () =
      traverse t (fun path cid -> map := String_map.add path cid !map)
    in
    Lwt.return !map

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

  (* produces a cid and cbor-encoded bytes for a given tree *)
  let serialize t node : (Cid.t * bytes, exn) Lwt_result.t =
    let sorted_entries =
      List.sort (fun (a : entry) b -> String.compare a.key b.key) node.entries
    in
    let rec aux node : (Cid.t * bytes) Lwt.t =
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
          node.entries
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
    try%lwt Lwt.map Result.ok (aux {node with entries= sorted_entries})
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
          match Util.at_index index seq with
          | Some (Leaf (k, v, _)) when k = key -> (
              (* include the found leaf block to prove existence *)
              match%lwt Store.get_bytes t.blockstore v with
              | Some leaf_bytes ->
                  Lwt.return (Block_map.set v leaf_bytes Block_map.empty)
              | None ->
                  Lwt.return Block_map.empty )
          | _ -> (
              let prev =
                if index - 1 >= 0 then Util.at_index (index - 1) seq else None
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
                    match Util.at_index index seq with
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
                            Lwt.return
                              (Block_map.set cid_left b Block_map.empty)
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

  (* returns all mst nodes needed to prove the value of a given key's left sibling *)
  let rec proof_for_left_sibling t cid key : Block_map.t Lwt.t =
    match%lwt Store.get_bytes t.blockstore cid with
    | None ->
        Lwt.return Block_map.empty
    | Some bytes ->
        let raw = decode_block_raw bytes in
        let keys = node_entry_keys raw in
        let seq = interleave_raw raw keys in
        let index = find_gte_leaf_index key seq in
        let%lwt blocks =
          let prev =
            if index - 1 >= 0 then Util.at_index (index - 1) seq else None
          in
          match prev with
          | Some (Tree c) ->
              proof_for_left_sibling t c key
          | Some (Leaf (_, v_left, _)) -> (
              match%lwt Store.get_bytes t.blockstore v_left with
              | Some b ->
                  Lwt.return (Block_map.set v_left b Block_map.empty)
              | None ->
                  Lwt.return Block_map.empty )
          | _ ->
              Lwt.return Block_map.empty
        in
        Lwt.return (Block_map.set cid bytes blocks)

  (* returns all mst nodes needed to prove the value of a given key's right sibling *)
  let rec proof_for_right_sibling t cid key : Block_map.t Lwt.t =
    match%lwt Store.get_bytes t.blockstore cid with
    | None ->
        Lwt.return Block_map.empty
    | Some bytes ->
        let raw = decode_block_raw bytes in
        let keys = node_entry_keys raw in
        let seq = interleave_raw raw keys in
        let index = find_gte_leaf_index key seq in
        let found =
          match Util.at_index index seq with
          | None ->
              if index - 1 >= 0 then Util.at_index (index - 1) seq else None
          | some ->
              some
        in
        let%lwt blocks =
          match found with
          | Some (Tree c) ->
              proof_for_right_sibling t c key
          | Some (Leaf (k, _, _)) -> (
              let neighbor =
                if k = key then Util.at_index (index + 1) seq
                else if index - 1 >= 0 then Util.at_index (index - 1) seq
                else None
              in
              match neighbor with
              | Some (Tree c) ->
                  proof_for_right_sibling t c key
              | Some (Leaf (_, v_right, _)) -> (
                  match%lwt Store.get_bytes t.blockstore v_right with
                  | Some b ->
                      Lwt.return (Block_map.set v_right b Block_map.empty)
                  | None ->
                      Lwt.return Block_map.empty )
              | _ ->
                  Lwt.return Block_map.empty )
          | None ->
              Lwt.return Block_map.empty
        in
        Lwt.return (Block_map.set cid bytes blocks)

  (* a covering proof is all mst nodes needed to prove the value of a given leaf
    and its siblings to its immediate right and left (if applicable) *)
  let get_covering_proof t key : Block_map.t Lwt.t =
    let%lwt proofs =
      Lwt.all
        [ proof_for_key t t.root key
        ; proof_for_left_sibling t t.root key
        ; proof_for_right_sibling t t.root key ]
    in
    Lwt.return
      (List.fold_left
         (fun acc proof -> Block_map.merge acc proof)
         Block_map.empty proofs )

  (* collects all node blocks (cid, bytes) and all leaf cids reachable from root
     only traverses nodes; doesn't fetch leaf blocks
     returns (nodes, visited, leaves) *)
  let collect_nodes_and_leaves t :
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
  let rec leaves_of_node n : (string * Cid.t) list Lwt.t =
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
  let leaves_of_root t : (string * Cid.t) list Lwt.t =
    match%lwt retrieve_node t t.root with
    | None ->
        failwith "root cid not found in repo store"
    | Some root ->
        leaves_of_node root

  (* returns a count of all leaves in the mst *)
  let leaf_count t : int Lwt.t =
    match%lwt retrieve_node t t.root with
    | None ->
        failwith "root cid not found in repo store"
    | Some root ->
        let rec count (n : node) : int Lwt.t =
          let%lwt left_count =
            n.left >>? function Some l -> count l | None -> Lwt.return 0
          in
          let%lwt right_counts =
            Lwt_list.map_s
              (fun (e : entry) ->
                e.right >>? function Some r -> count r | None -> Lwt.return 0 )
              n.entries
          in
          let sum_right = List.fold_left ( + ) 0 right_counts in
          Lwt.return (left_count + List.length n.entries + sum_right)
        in
        count root

  (* returns height of mst root *)
  let layer t : int Lwt.t =
    match%lwt retrieve_node t t.root with
    | None ->
        failwith "root cid not found in repo store"
    | Some root ->
        Lwt.return root.layer

  (* returns all nodes sorted by cid *)
  let all_nodes t : (Cid.t * bytes) list Lwt.t =
    let rec bfs (queue : Cid.t list) (visited : Cid.Set.t)
        (nodes : (Cid.t * bytes) list) =
      match queue with
      | [] ->
          Lwt.return nodes
      | cid :: rest -> (
          if Cid.Set.mem cid visited then bfs rest visited nodes
          else
            match%lwt Store.get_bytes t.blockstore cid with
            | None ->
                failwith ("missing mst node block: " ^ Cid.to_string cid)
            | Some bytes ->
                let raw = decode_block_raw bytes in
                let next_cids =
                  let acc = match raw.l with Some l -> [l] | None -> [] in
                  List.fold_left
                    (fun acc e ->
                      match e.t with Some r -> r :: acc | None -> acc )
                    acc raw.e
                in
                let visited' = Cid.Set.add cid visited in
                bfs
                  (List.rev_append next_cids rest)
                  visited' ((cid, bytes) :: nodes) )
    in
    let%lwt nodes = bfs [t.root] Cid.Set.empty [] in
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

  (* returns the cid for a given key, if it exists *)
  let get_cid t key : Cid.t option Lwt.t =
    let rec get_in_node (n : node) : Cid.t option Lwt.t =
      let sorted_entries =
        List.sort
          (fun (a : entry) (b : entry) -> String.compare a.key b.key)
          n.entries
      in
      let rec scan (prev : entry option) (entries : entry list) :
          Cid.t option Lwt.t =
        match entries with
        | [] -> (
          match prev with
          | Some p -> (
              p.right
              >>? function Some r -> get_in_node r | None -> Lwt.return_none )
          | None -> (
              n.left
              >>? function Some l -> get_in_node l | None -> Lwt.return_none ) )
        | e :: rest ->
            if key = e.key then Lwt.return_some e.value
            else if key < e.key then
              match prev with
              | Some p -> (
                  p.right
                  >>? function
                  | Some r ->
                      get_in_node r
                  | None ->
                      Lwt.return_none )
              | None -> (
                  n.left
                  >>? function
                  | Some l ->
                      get_in_node l
                  | None ->
                      Lwt.return_none )
            else scan (Some e) rest
      in
      scan None sorted_entries
    in
    match%lwt retrieve_node t t.root with
    | None ->
        Lwt.fail (Invalid_argument "root cid not found in repo store")
    | Some root ->
        get_in_node root

  (* builds and persists a canonical mst from sorted leaves *)
  let of_assoc blockstore assoc : t Lwt.t =
    let open Lwt.Infix in
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
          Store.put_block blockstore cid encoded >|= fun _ -> (cid, 0)
      | _ ->
          let with_layers =
            List.map (fun (k, v) -> (k, v, Util.leading_zeros_on_hash k)) pairs
          in
          let root_layer =
            List.fold_left (fun acc (_, _, lz) -> max acc lz) 0 with_layers
          in
          let on_layer =
            List.filter (fun (_, _, lz) -> lz = root_layer) with_layers
            |> List.map (fun (k, v, _) -> (k, v))
          in
          (* left group is keys below first on-layer key *)
          let left_group =
            match on_layer with
            | (k0, _) :: _ ->
                List.filter
                  (fun (k, _, lz) -> lz < root_layer && k < k0)
                  with_layers
                |> List.map (fun (k, v, _) -> (k, v))
            | [] ->
                []
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
                    Store.put_block blockstore cid' encoded
                    >>= fun _ -> wrap cid' (layer + 1)
                in
                wrap cid child_layer >|= fun c -> Some c
          in
          (* compute right groups aligned to on-layer entries *)
          let rec right_groups acc rest =
            match rest with
            | [] ->
                List.rev acc
            | (k, _) :: tl ->
                let upper =
                  match tl with (k2, _) :: _ -> Some k2 | [] -> None
                in
                let grp =
                  List.filter
                    (fun (k', _, lz) ->
                      lz < root_layer && k' > k
                      && match upper with Some ku -> k' < ku | None -> true )
                    with_layers
                  |> List.map (fun (k', v', _) -> (k', v'))
                in
                right_groups ((k, grp) :: acc) tl
          in
          let rights = right_groups [] on_layer in
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
                        Store.put_block blockstore cid' encoded
                        >>= fun _ -> wrap cid' (layer + 1)
                    in
                    wrap cid child_layer >|= fun c -> Some c )
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
          Store.put_block blockstore cid encoded >|= fun _ -> (cid, root_layer)
    in
    persist_from_sorted sorted >|= fun (root, _) -> {blockstore; root}

  (* insert or replace an entry, returning a new canonical mst
     doesn't persist changes *)
  let add t key cid : t Lwt.t =
    Util.ensure_valid_key key ;
    let%lwt leaves = leaves_of_root t in
    let without = List.filter (fun (k, _) -> k <> key) leaves in
    of_assoc t.blockstore ((key, cid) :: without)

  (* delete an entry, returning a new canonical mst
     doesn't persist changes *)
  let delete t key : t Lwt.t =
    Util.ensure_valid_key key ;
    let%lwt leaves = leaves_of_root t in
    let remaining = List.filter (fun (k, _) -> k <> key) leaves in
    of_assoc t.blockstore remaining

  (* produces a diff from an empty mst to the current one *)
  let null_diff curr : data_diff Lwt.t =
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

module Differ (Prev : Intf) (Curr : Intf) = struct
  let diff ~(t_curr : Curr.t) ~(t_prev : Prev.t) : data_diff Lwt.t =
    let%lwt curr_nodes, curr_node_set, curr_leaf_set =
      Curr.collect_nodes_and_leaves t_curr
    in
    let%lwt _, prev_node_set, prev_leaf_set =
      Prev.collect_nodes_and_leaves t_prev
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
    let%lwt curr_leaves = Curr.leaves_of_root t_curr in
    let%lwt prev_leaves = Prev.leaves_of_root t_prev in
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
              merge pr cr adds ({key= k1; prev= c1; cid= c2} :: updates) deletes
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
end

module Inductive (M : Intf) = struct
  module Mem_mst = Make (Memory_blockstore)

  type diff =
    | Add of {key: string; cid: Cid.t}
    | Update of {key: string; prev: Cid.t option; cid: Cid.t}
    | Delete of {key: string; prev: Cid.t}

  (* given an mst diff, returns all new blocks as well as inductive proof blocks *)
  let generate_proof (curr : M.t) (diff : diff list) (prev_root : Cid.t) :
      ((Cid.t * bytes) list, exn) Lwt_result.t =
    try%lwt
      let%lwt map = M.build_map curr in
      let%lwt mem_mst =
        Mem_mst.of_assoc (Memory_blockstore.create ()) (String_map.bindings map)
      in
      (* track all accessed cids relevant to inductive proof *)
      let accessed_cids = ref Cid.Set.empty in
      (* apply inverse of operations in reverse order *)
      let%lwt new_mst, added_cids =
        Lwt_list.fold_right_s
          (fun (diff : diff) (mst, added_cids) ->
            match diff with
            | Delete {key; prev} | Update {key; prev= Some prev; _} ->
                accessed_cids := Cid.Set.add prev !accessed_cids ;
                let%lwt mst = Mem_mst.add mst key prev in
                Lwt.return (mst, Cid.Set.remove prev added_cids)
            | Add {key; cid} | Update {key; prev= None; cid} ->
                accessed_cids := Cid.Set.add cid !accessed_cids ;
                let%lwt mst = Mem_mst.delete mst key in
                Lwt.return (mst, Cid.Set.add cid added_cids) )
          diff (mem_mst, Cid.Set.empty)
      in
      if not (Cid.equal new_mst.root prev_root) then
        failwith
          (Printf.sprintf
             "inductive proof produced invalid previous cid: expected %s, got \
              %s"
             (Cid.to_string prev_root)
             (Cid.to_string new_mst.root) ) ;
      let proof_cids =
        Cid.Set.union added_cids !accessed_cids
        |> Cid.Set.remove prev_root |> Cid.Set.add new_mst.root
      in
      let%lwt {blocks= proof_bm; _} =
        Memory_blockstore.get_blocks mem_mst.blockstore
          (Cid.Set.elements proof_cids)
      in
      Lwt.return_ok (Block_map.entries proof_bm)
    with e -> Lwt.return_error e
end
