open Mist
open Lwt.Infix
open Lwt_result.Syntax
module Mem_mst = Mst.Make (Storage.Memory_blockstore)
module String_map = Dag_cbor.String_map

type diff_add = {key: string; cid: Cid.t}

type diff_update = {key: string; prev: Cid.t; cid: Cid.t}

type diff_delete = {key: string; cid: Cid.t}

type data_diff =
  { adds: diff_add list
  ; updates: diff_update list
  ; deletes: diff_delete list
  ; new_mst_blocks: (Cid.t * bytes) list
  ; new_leaf_cids: Cid.Set.t }

module Differ (Prev : Mst.Intf) (Curr : Mst.Intf) = struct
  let diff ~(t_curr : Curr.t) ~(t_prev : Prev.t) : data_diff Lwt.t =
    let%lwt curr_nodes, _, curr_leaf_set =
      Curr.collect_nodes_and_leaves t_curr
    in
    let%lwt _, prev_node_set, prev_leaf_set =
      Prev.collect_nodes_and_leaves t_prev
    in
    let in_prev_nodes cid = Cid.Set.mem cid prev_node_set in
    let in_prev_leaves cid = Cid.Set.mem cid prev_leaf_set in
    let new_mst_blocks =
      List.filter (fun (cid, _) -> not (in_prev_nodes cid)) curr_nodes
    in
    let new_leaf_cids =
      Cid.Set.fold
        (fun cid acc ->
          if not (in_prev_leaves cid) then Cid.Set.add cid acc else acc )
        curr_leaf_set Cid.Set.empty
    in
    let%lwt curr_leaves = Curr.leaves_of_root t_curr in
    let%lwt prev_leaves = Prev.leaves_of_root t_prev in
    let rec merge (pl : (string * Cid.t) list) (cl : (string * Cid.t) list)
        (adds : diff_add list) (updates : diff_update list)
        (deletes : diff_delete list) =
      match (pl, cl) with
      | [], [] ->
          (List.rev adds, List.rev updates, List.rev deletes)
      | [], (k, c) :: cr ->
          merge [] cr ({key= k; cid= c} :: adds) updates deletes
      | (k, c) :: pr, [] ->
          merge pr [] adds updates ({key= k; cid= c} :: deletes)
      | (k1, c1) :: pr, (k2, c2) :: cr ->
          if k1 = k2 then
            if Cid.equal c1 c2 then merge pr cr adds updates deletes
            else
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
    Lwt.return {adds; updates; deletes; new_mst_blocks; new_leaf_cids}
end

module Mem_diff = Differ (Mem_mst) (Mem_mst)

let cid_of_string_exn s =
  match Cid.of_string s with Ok c -> c | Error msg -> failwith msg

let test_trims_top_on_delete () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let l1root = "bafyreifnqrwbk6ffmyaz5qtujqrzf5qmxf7cbxvgzktl4e3gabuxbtatv4" in
  let l0root = "bafyreie4kjuxbwkhzg2i5dljaswcroeih4dgiqq6pazcmunwt2byd725vi" in
  let* mst = Mem_mst.create_empty store in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fn2j" cid1 in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fo2j" cid1 in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fp2j" cid1 in
  (* level 1 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fs2j" cid1 in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3ft2j" cid1 in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fu2j" cid1 in
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (before delete)" 6 cnt ;
  let%lwt layer_before = Mem_mst.layer mst in
  Alcotest.(check int) "root layer before delete" 1 layer_before ;
  let root_before = mst.root in
  Alcotest.(check string)
    "root cid before delete" l1root
    (Cid.to_string root_before) ;
  (* delete level 1 entry, expect trimming to layer 0 *)
  let%lwt mst' = Mem_mst.delete mst "com.example.record/3jqfcqzm3fs2j" in
  let%lwt cnt' = Mem_mst.leaf_count mst' in
  Alcotest.(check int) "leaf count (after delete)" 5 cnt' ;
  let%lwt layer_after = Mem_mst.layer mst' in
  Alcotest.(check int) "root layer after delete" 0 layer_after ;
  let root_after = mst'.root in
  Alcotest.(check string)
    "root cid after delete" l0root (Cid.to_string root_after) ;
  Lwt.return_ok ()

let test_insertion_splits_two_layers_down () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let l1root = "bafyreiettyludka6fpgp33stwxfuwhkzlur6chs4d2v4nkmq2j3ogpdjem" in
  let l2root = "bafyreid2x5eqs4w4qxvc5jiwda4cien3gw2q6cshofxwnvv7iucrmfohpm" in
  let* mst = Mem_mst.create_empty store in
  (* A; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fo2j" cid1 in
  (* B; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fp2j" cid1 in
  (* C; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fr2j" cid1 in
  (* D; level 1 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fs2j" cid1 in
  (* E; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3ft2j" cid1 in
  (* G; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fz2j" cid1 in
  (* H; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm4fc2j" cid1 in
  (* I; level 1 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm4fd2j" cid1 in
  (* J; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm4ff2j" cid1 in
  (* K; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm4fg2j" cid1 in
  (* L; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm4fh2j" cid1 in
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (before F)" 11 cnt ;
  let%lwt layer_before = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (before F)" 1 layer_before ;
  let root_before = mst.root in
  Alcotest.(check string)
    "root cid (before F)" l1root
    (Cid.to_string root_before) ;
  (* insert F; level 2 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fx2j" cid1 in
  let%lwt cnt_after_f = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (after F)" 12 cnt_after_f ;
  let%lwt layer_after_f = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (after F)" 2 layer_after_f ;
  let root_after_f = mst.root in
  Alcotest.(check string)
    "root cid (after F)" l2root
    (Cid.to_string root_after_f) ;
  (* remove F; should return to previous root/layer *)
  let%lwt mst = Mem_mst.delete mst "com.example.record/3jqfcqzm3fx2j" in
  let%lwt cnt_after_del_f = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (after del F)" 11 cnt_after_del_f ;
  let%lwt layer_after_del_f = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (after del F)" 1 layer_after_del_f ;
  let root_after_del_f = mst.root in
  Alcotest.(check string)
    "root cid (after del F)" l1root
    (Cid.to_string root_after_del_f) ;
  Lwt.return_ok ()

let test_new_layers_two_higher_than_existing () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let l0root = "bafyreidfcktqnfmykz2ps3dbul35pepleq7kvv526g47xahuz3rqtptmky" in
  let l2root = "bafyreiavxaxdz7o7rbvr3zg2liox2yww46t7g6hkehx4i4h3lwudly7dhy" in
  let l2root2 = "bafyreig4jv3vuajbsybhyvb7gggvpwh2zszwfyttjrj6qwvcsp24h6popu" in
  let* mst = Mem_mst.create_empty store in
  (* A; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3ft2j" cid1 in
  (* C; level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fz2j" cid1 in
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,C)" 2 cnt ;
  let%lwt layer_ac = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (A,C)" 0 layer_ac ;
  let root_ac = mst.root in
  Alcotest.(check string) "root cid (A,C)" l0root (Cid.to_string root_ac) ;
  (* insert B (level 2) *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fx2j" cid1 in
  let%lwt cnt_abc = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,B,C)" 3 cnt_abc ;
  let%lwt layer_abc = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (A,B,C)" 2 layer_abc ;
  let root_abc = mst.root in
  Alcotest.(check string) "root cid (A,B,C)" l2root (Cid.to_string root_abc) ;
  (* remove B → back to l0root *)
  let%lwt mst = Mem_mst.delete mst "com.example.record/3jqfcqzm3fx2j" in
  let%lwt cnt_ac = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,C again)" 2 cnt_ac ;
  let%lwt layer_ac2 = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (A,C again)" 0 layer_ac2 ;
  let root_ac2 = mst.root in
  Alcotest.(check string) "root cid (A,C again)" l0root (Cid.to_string root_ac2) ;
  (* insert B (level 2) and D (level 1) *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fx2j" cid1 in
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm4fd2j" cid1 in
  let%lwt cnt_abcd = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,B,C,D)" 4 cnt_abcd ;
  let%lwt layer_abcd = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (A,B,C,D)" 2 layer_abcd ;
  let root_abcd = mst.root in
  Alcotest.(check string) "root cid (A,B,C,D)" l2root2 (Cid.to_string root_abcd) ;
  (* remove D → match l2root *)
  let%lwt mst = Mem_mst.delete mst "com.example.record/3jqfcqzm4fd2j" in
  let%lwt cnt_abc2 = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,B,C again)" 3 cnt_abc2 ;
  let%lwt layer_abc2 = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (A,B,C again)" 2 layer_abc2 ;
  let root_abc2 = mst.root in
  Alcotest.(check string)
    "root cid (A,B,C again)" l2root (Cid.to_string root_abc2) ;
  Lwt.return_ok ()

let rand_bytes n =
  let b = Bytes.create n in
  for i = 0 to n - 1 do
    Bytes.set b i (Char.chr (Random.int 256))
  done ;
  b

let random_block () =
  let b = rand_bytes 64 in
  let bytes = Dag_cbor.encode (`Bytes b) in
  let cid = Cid.create Dcbor bytes in
  (cid, bytes)

let put_random_block store =
  let cid, bytes = random_block () in
  Storage.Memory_blockstore.put_block store cid bytes >|= fun _ -> cid

let random_alnum len =
  let allowed = "abcdefghijklmnopqrstuvwxyz0123456789" in
  let b = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.set b i allowed.[Random.int (String.length allowed)]
  done ;
  Bytes.to_string b

let valid_rkey () = random_alnum 13

let make_key () = "com.example/" ^ valid_rkey ()

let rec unique_keys n acc =
  if n <= 0 then acc
  else
    let k = make_key () in
    if List.mem k acc then unique_keys n acc else unique_keys (n - 1) (k :: acc)

let generate_bulk_data_keys store count =
  let keys = unique_keys count [] in
  Lwt_list.fold_left_s
    (fun acc k -> put_random_block store >|= fun cid -> String_map.add k cid acc)
    String_map.empty keys

let shuffle lst =
  let arr = Array.of_list lst in
  for i = Array.length arr - 1 downto 1 do
    let j = Random.int (i + 1) in
    let tmp = arr.(i) in
    arr.(i) <- arr.(j) ;
    arr.(j) <- tmp
  done ;
  Array.to_list arr

let assoc_of_map m = String_map.bindings m

let rec take n lst =
  match (n, lst) with
  | 0, _ ->
      []
  | _, [] ->
      []
  | n, x :: xs ->
      x :: take (n - 1) xs

let rec drop n lst =
  match (n, lst) with
  | 0, _ ->
      lst
  | _, [] ->
      []
  | n, _ :: xs ->
      drop (n - 1) xs

let () = Random.self_init ()

let test_adds () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst' =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst shuffled
  in
  let%lwt result_map = Mem_mst.build_map mst' in
  List.iter
    (fun (k, v) ->
      let got = String_map.find_opt k result_map in
      Alcotest.(check bool)
        "added records retrievable" true
        (Option.value (Option.map (fun x -> Cid.equal v x) got) ~default:false) )
    shuffled ;
  let%lwt total = Mem_mst.leaf_count mst' in
  Alcotest.(check int) "leaf count after adds" 1000 total ;
  Lwt.return_ok ()

let test_edits () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst shuffled
  in
  let to_edit = take 100 shuffled in
  let%lwt edited_mst, edited =
    Lwt_list.fold_left_s
      (fun (t, acc) (k, _old) ->
        let%lwt new_cid = put_random_block store in
        Mem_mst.add t k new_cid >|= fun t' -> (t', (k, new_cid) :: acc) )
      (mst, []) to_edit
  in
  let edited = List.rev edited in
  let%lwt result_map = Mem_mst.build_map edited_mst in
  List.iter
    (fun (k, v) ->
      let got = String_map.find_opt k result_map in
      Alcotest.(check bool)
        "updated records retrievable" true
        (Option.value (Option.map (fun x -> Cid.equal v x) got) ~default:false) )
    edited ;
  let%lwt total = Mem_mst.leaf_count edited_mst in
  Alcotest.(check int) "leaf count stable after edits" 1000 total ;
  Lwt.return_ok ()

let test_deletes () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst shuffled
  in
  let to_delete, the_rest =
    let rec split n acc rest =
      match (n, rest) with
      | 0, _ ->
          (List.rev acc, rest)
      | _, x :: xs ->
          split (n - 1) (x :: acc) xs
      | _, [] ->
          (List.rev acc, [])
    in
    split 100 [] shuffled
  in
  let%lwt deleted_mst =
    Lwt_list.fold_left_s (fun t (k, _) -> Mem_mst.delete t k) mst to_delete
  in
  let%lwt total = Mem_mst.leaf_count deleted_mst in
  Alcotest.(check int) "leaf count after deletes" 900 total ;
  let%lwt result_map = Mem_mst.build_map deleted_mst in
  List.iter
    (fun (k, _) ->
      let got = String_map.find_opt k result_map in
      Alcotest.(check bool) "deleted record missing" true (got = None) )
    to_delete ;
  List.iter
    (fun (k, v) ->
      let got = String_map.find_opt k result_map in
      Alcotest.(check bool)
        "remaining records intact" true
        (Option.value (Option.map (fun x -> Cid.equal v x) got) ~default:false) )
    the_rest ;
  Lwt.return_ok ()

let test_order_independent () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst shuffled
  in
  let%lwt all_nodes = Mem_mst.all_nodes mst in
  let* recreated = Mem_mst.create_empty store in
  let reshuffled = shuffle (assoc_of_map mapping) in
  let%lwt recreated =
    Lwt_list.fold_left_s
      (fun t (k, v) -> Mem_mst.add t k v)
      recreated reshuffled
  in
  let%lwt all_reshuffled = Mem_mst.all_nodes recreated in
  Alcotest.(check int)
    "node count equal" (List.length all_nodes)
    (List.length all_reshuffled) ;
  List.iter2
    (fun (cid1, bytes1) (cid2, bytes2) ->
      Alcotest.(check bool) "cid equal" true (Cid.equal cid1 cid2) ;
      Alcotest.(check string)
        "bytes equal" (Bytes.to_string bytes1) (Bytes.to_string bytes2) )
    all_nodes all_reshuffled ;
  Lwt.return_ok ()

let test_save_load () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 300 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst shuffled
  in
  let loaded = Mem_mst.create store mst.root in
  let%lwt orig_nodes = Mem_mst.all_nodes mst in
  let%lwt loaded_nodes = Mem_mst.all_nodes loaded in
  Alcotest.(check int)
    "node count equal" (List.length orig_nodes) (List.length loaded_nodes) ;
  List.iter2
    (fun (cid1, bytes1) (cid2, bytes2) ->
      Alcotest.(check bool) "cid equal" true (Cid.equal cid1 cid2) ;
      Alcotest.(check string)
        "bytes equal" (Bytes.to_string bytes1) (Bytes.to_string bytes2) )
    orig_nodes loaded_nodes ;
  Lwt.return_ok ()

let test_diffs () =
  let store = Storage.Memory_blockstore.create () in
  let* mst0 = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst0 shuffled
  in
  (* additions *)
  let%lwt add_map = generate_bulk_data_keys store 100 in
  let to_add = assoc_of_map add_map in
  (* edits from existing *)
  let to_edit = take 100 (drop 500 shuffled) in
  (* deletes from existing *)
  let to_del = take 100 (drop 400 shuffled) in
  let expected_adds =
    List.fold_left
      (fun m (k, v) -> String_map.add k v m)
      String_map.empty to_add
  in
  let%lwt to_diff, expected_updates =
    Lwt_list.fold_left_s
      (fun (t, m) (k, old_v) ->
        let%lwt updated = put_random_block store in
        let m' = String_map.add k (old_v, updated) m in
        Mem_mst.add t k updated >|= fun t' -> (t', m') )
      (mst, String_map.empty) to_edit
  in
  let%lwt to_diff, expected_dels =
    Lwt_list.fold_left_s
      (fun (t, m) (k, v) ->
        Mem_mst.delete t k >|= fun t' -> (t', String_map.add k v m) )
      (to_diff, String_map.empty)
      to_del
  in
  let%lwt to_diff =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) to_diff to_add
  in
  let%lwt diff = Mem_diff.diff ~t_curr:to_diff ~t_prev:mst in
  (* lengths *)
  Alcotest.(check int) "adds length" 100 (List.length diff.adds) ;
  Alcotest.(check int) "updates length" 100 (List.length diff.updates) ;
  Alcotest.(check int) "deletes length" 100 (List.length diff.deletes) ;
  (* contents: convert to maps to compare *)
  let adds_map =
    List.fold_left
      (fun m (a : diff_add) -> String_map.add a.key a.cid m)
      String_map.empty diff.adds
  in
  let updates_map =
    List.fold_left
      (fun m (u : diff_update) -> String_map.add u.key (u.prev, u.cid) m)
      String_map.empty diff.updates
  in
  let deletes_map =
    List.fold_left
      (fun m (d : diff_delete) -> String_map.add d.key d.cid m)
      String_map.empty diff.deletes
  in
  (* compare adds *)
  Alcotest.(check int)
    "adds map size equal"
    (String_map.cardinal expected_adds)
    (String_map.cardinal adds_map) ;
  String_map.iter
    (fun k v ->
      match String_map.find_opt k adds_map with
      | Some v' ->
          Alcotest.(check bool) "add cid equal" true (Cid.equal v v')
      | None ->
          Alcotest.failf "missing add key %s" k )
    expected_adds ;
  (* compare updates *)
  Alcotest.(check int)
    "updates map size equal"
    (String_map.cardinal expected_updates)
    (String_map.cardinal updates_map) ;
  String_map.iter
    (fun k (prev, cid) ->
      match String_map.find_opt k updates_map with
      | Some (prev', cid') ->
          Alcotest.(check bool) "update prev equal" true (Cid.equal prev prev') ;
          Alcotest.(check bool) "update cid equal" true (Cid.equal cid cid')
      | None ->
          Alcotest.failf "missing update key %s" k )
    expected_updates ;
  (* compare deletes *)
  Alcotest.(check int)
    "deletes map size equal"
    (String_map.cardinal expected_dels)
    (String_map.cardinal deletes_map) ;
  String_map.iter
    (fun k v ->
      match String_map.find_opt k deletes_map with
      | Some v' ->
          Alcotest.(check bool) "delete cid equal" true (Cid.equal v v')
      | None ->
          Alcotest.failf "missing delete key %s" k )
    expected_dels ;
  (* ensure we correctly report all added CIDs *)
  let%lwt leaves = Mem_mst.leaves_of_root to_diff in
  let node_cid_set =
    List.fold_left
      (fun s (c, _) -> Cid.Set.add c s)
      Cid.Set.empty diff.new_mst_blocks
  in
  let%lwt () =
    Lwt_list.iter_s
      (fun (_k, cid) ->
        let%lwt has = Storage.Memory_blockstore.has store cid in
        let found =
          has
          || Cid.Set.mem cid diff.new_leaf_cids
          || Cid.Set.mem cid node_cid_set
        in
        Alcotest.(check bool) "leaf cid accounted for" true found |> Lwt.return )
      leaves
  in
  Lwt.return_ok ()

let test_allowable_keys () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let cid1 =
    Cid.of_string "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
    |> Result.get_ok
  in
  let expect_reject key =
    Lwt.catch
      (fun () ->
        Mem_mst.add mst key cid1
        >>= fun _ ->
        Alcotest.failf "expected invalid key to be rejected: %s" key )
      (function
        | Invalid_argument _ ->
            Lwt.return_ok ()
        | exn ->
            Alcotest.failf "unexpected exception for %s: %s" key
              (Printexc.to_string exn) )
  in
  let expect_allow key = Mem_mst.add mst key cid1 >|= fun _ -> () in
  let* () = expect_reject "" in
  let* () = expect_reject "asdf" in
  let* () = expect_reject "nested/collection/asdf" in
  let* () = expect_reject "coll/" in
  let* () = expect_reject "/rkey" in
  let* () = expect_reject "coll/jalapeñoA" in
  let* () = expect_reject "coll/coöperative" in
  let* () = expect_reject "coll/abc💩" in
  let* () = expect_reject "coll/key$" in
  let* () = expect_reject "coll/key%" in
  let* () = expect_reject "coll/key(" in
  let* () = expect_reject "coll/key)" in
  let* () = expect_reject "coll/key+" in
  let* () = expect_reject "coll/key=" in
  let* () = expect_reject "coll/@handle" in
  let* () = expect_reject "coll/any space" in
  let* () = expect_reject "coll/#extra" in
  let* () = expect_reject "coll/any+space" in
  let* () = expect_reject "coll/number[3]" in
  let* () = expect_reject "coll/number(3)" in
  let* () = expect_reject "coll/dHJ1ZQ==" in
  let* () = expect_reject "coll/\"quote\"" in
  let big =
    "coll/"
    ^ String.concat ""
        (Array.to_list
           (Array.init 1100 (fun _ ->
                String.make 1 (Char.chr (97 + Random.int 26)) ) ) )
  in
  let* () = expect_reject big in
  let%lwt () = expect_allow "coll/3jui7kd54zh2y" in
  let%lwt () = expect_allow "coll/self" in
  let%lwt () = expect_allow "coll/example.com" in
  let%lwt () = expect_allow "com.example/rkey" in
  let%lwt () = expect_allow "coll/~1.2-3_" in
  let%lwt () = expect_allow "coll/dHJ1ZQ" in
  let%lwt () = expect_allow "coll/pre:fix" in
  let%lwt () = expect_allow "coll/_" in
  Lwt.return_ok ()

let test_empty_root () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (empty)" 0 cnt ;
  Alcotest.(check string)
    "empty root cid"
    "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"
    (Cid.to_string mst.root) ;
  Lwt.return_ok ()

let test_trivial_root () =
  let store : Mem_mst.Store.t = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let* mst = Mem_mst.create_empty store in
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fo2j" cid1 in
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (trivial)" 1 cnt ;
  Alcotest.(check string)
    "trivial root cid"
    "bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu"
    (Cid.to_string mst.root) ;
  Lwt.return_ok ()

let test_singlelayer2_root () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let* mst = Mem_mst.create_empty store in
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fx2j" cid1 in
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (singlelayer2)" 1 cnt ;
  let%lwt layer = Mem_mst.layer mst in
  Alcotest.(check int) "root layer (singlelayer2)" 2 layer ;
  Alcotest.(check string)
    "singlelayer2 root cid"
    "bafyreih7wfei65pxzhauoibu3ls7jgmkju4bspy4t2ha2qdjnzqvoy33ai"
    (Cid.to_string mst.root) ;
  Lwt.return_ok ()

let test_simple_root () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let* mst = Mem_mst.create_empty store in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fp2j" cid1 in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fr2j" cid1 in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fs2j" cid1 in
  (* level 1 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3ft2j" cid1 in
  (* level 0 *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm4fc2j" cid1 in
  (* level 0 *)
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "leaf count (simple)" 5 cnt ;
  Alcotest.(check string)
    "simple root cid"
    "bafyreicmahysq4n6wfuxo522m6dpiy7z7qzym3dzs756t5n7nfdgccwq7m"
    (Cid.to_string mst.root) ;
  Lwt.return_ok ()

let test_roundtrip () =
  let mst_of_car_bytes bytes =
    let%lwt roots, blocks = Car.read_car_stream (Lwt_seq.of_list [bytes]) in
    let root =
      match roots with
      | [root] ->
          root
      | _ ->
          failwith "expected exactly one root in car stream"
    in
    let%lwt blocks_list = Lwt_seq.to_list blocks in
    let bm =
      Seq.fold_left
        (fun acc (cid, bytes) -> Storage.Block_map.set cid bytes acc)
        Storage.Block_map.empty (List.to_seq blocks_list)
    in
    let store = Storage.Memory_blockstore.create ~blocks:bm () in
    let%lwt commit =
      match%lwt Storage.Memory_blockstore.get_bytes store root with
      | Some b -> (
        match Dag_cbor.decode b with
        | `Map commit ->
            Lwt.return commit
        | _ ->
            failwith "non-object commit" )
      | None ->
          failwith "root not found in blockstore"
    in
    let mst =
      Mem_mst.create store
        ( match String_map.find "data" commit with
        | `Link cid ->
            cid
        | _ ->
            failwith "non-cid data in commit" )
    in
    Lwt.return (commit, mst)
  in
  let%lwt ic = Lwt_io.open_file ~mode:Lwt_io.input "sample.car" in
  let%lwt car = Lwt_io.read ic >|= Bytes.of_string in
  let%lwt () = Lwt_io.close ic in
  let%lwt commit, mst = mst_of_car_bytes car in
  let mst_stream = Mem_mst.to_blocks_stream mst in
  let commit_bytes = Dag_cbor.encode (`Map commit) in
  let commit_cid = Cid.create Dcbor commit_bytes in
  let%lwt car' =
    Car.blocks_to_car commit_cid
      (Lwt_seq.append (Lwt_seq.of_list [(commit_cid, commit_bytes)]) mst_stream)
  in
  let%lwt _, mst' = mst_of_car_bytes car' in
  let%lwt eq = Mem_mst.equal mst mst' in
  Lwt.return (Alcotest.(check bool) "mst roundtrip" true eq)

let test_incremental_add_canonicity () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 200 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst_full =
    Lwt_list.fold_left_s
      (fun t (k, v) -> Mem_mst.add_rebuild t k v)
      mst shuffled
  in
  let* mst_inc = Mem_mst.create_empty store in
  let%lwt mst_inc =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst_inc shuffled
  in
  Alcotest.(check bool)
    "add produces same root as full rebuild" true
    (Cid.equal mst_full.root mst_inc.root) ;
  let%lwt full_leaves = Mem_mst.leaves_of_root mst_full in
  let%lwt inc_leaves = Mem_mst.leaves_of_root mst_inc in
  Alcotest.(check int)
    "same leaf count" (List.length full_leaves) (List.length inc_leaves) ;
  Lwt.return_ok ()

let test_incremental_delete_canonicity () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 200 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst shuffled
  in
  let to_delete = take 50 shuffled in
  let%lwt mst_full =
    Lwt_list.fold_left_s
      (fun t (k, _) -> Mem_mst.delete_rebuild t k)
      mst to_delete
  in
  let%lwt mst_inc =
    Lwt_list.fold_left_s (fun t (k, _) -> Mem_mst.delete t k) mst to_delete
  in
  Alcotest.(check bool)
    "delete produces same root as full rebuild" true
    (Cid.equal mst_full.root mst_inc.root) ;
  let%lwt full_leaves = Mem_mst.leaves_of_root mst_full in
  let%lwt inc_leaves = Mem_mst.leaves_of_root mst_inc in
  Alcotest.(check int)
    "same leaf count after delete" (List.length full_leaves)
    (List.length inc_leaves) ;
  Lwt.return_ok ()

let test_incremental_mixed_ops_canonicity () =
  let store = Storage.Memory_blockstore.create () in
  let* mst = Mem_mst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 100 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst shuffled
  in
  let%lwt more_mapping = generate_bulk_data_keys store 50 in
  let more_shuffled = shuffle (assoc_of_map more_mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> Mem_mst.add t k v) mst more_shuffled
  in
  let to_delete = take 30 shuffled in
  let%lwt mst_inc =
    Lwt_list.fold_left_s (fun t (k, _) -> Mem_mst.delete t k) mst to_delete
  in
  let* mst_ref = Mem_mst.create_empty store in
  let remaining_original =
    List.filter (fun (k, _) -> not (List.mem_assoc k to_delete)) shuffled
  in
  let all_keys = remaining_original @ assoc_of_map more_mapping in
  let%lwt mst_ref =
    Lwt_list.fold_left_s
      (fun t (k, v) -> Mem_mst.add_rebuild t k v)
      mst_ref all_keys
  in
  Alcotest.(check bool)
    "mixed ops produce same root as with rebuild" true
    (Cid.equal mst_ref.root mst_inc.root) ;
  Lwt.return_ok ()

let test_incremental_edge_cases () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let* mst = Mem_mst.create_empty store in
  (* add to empty tree *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fo2j" cid1 in
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "single add to empty" 1 cnt ;
  (* delete only entry *)
  let%lwt mst = Mem_mst.delete mst "com.example.record/3jqfcqzm3fo2j" in
  let%lwt cnt = Mem_mst.leaf_count mst in
  Alcotest.(check int) "delete returns to empty" 0 cnt ;
  (* delete nonexistent key *)
  let%lwt mst' = Mem_mst.delete mst "com.example.record/nonexistent" in
  Alcotest.(check bool)
    "delete non-existent returns same root" true
    (Cid.equal mst.root mst'.root) ;
  (* update existing key *)
  let cid2 =
    cid_of_string_exn
      "bafyreib2rxk3rybloqtdv5vwis645zhjcvdxjrrpa5hg2snpnf6asvgx6i"
  in
  let%lwt mst = Mem_mst.add mst "com.example/key1" cid1 in
  let%lwt mst = Mem_mst.add mst "com.example/key1" cid2 in
  let%lwt result_map = Mem_mst.build_map mst in
  ( match String_map.find_opt "com.example/key1" result_map with
  | Some cid ->
      Alcotest.(check bool) "update replaces value" true (Cid.equal cid cid2)
  | None ->
      Alcotest.fail "key should exist after update" ) ;
  Lwt.return_ok ()

let test_get_min_max_keys () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let* mst = Mem_mst.create_empty store in
  (* empty tree *)
  let%lwt min_empty = Mem_mst.get_min_key mst mst.root in
  let%lwt max_empty = Mem_mst.get_max_key mst mst.root in
  Alcotest.(check (option string)) "empty min" None min_empty ;
  Alcotest.(check (option string)) "empty max" None max_empty ;
  (* single entry *)
  let%lwt mst = Mem_mst.add mst "com.example/mmm" cid1 in
  let%lwt min_single = Mem_mst.get_min_key mst mst.root in
  let%lwt max_single = Mem_mst.get_max_key mst mst.root in
  Alcotest.(check (option string))
    "single min" (Some "com.example/mmm") min_single ;
  Alcotest.(check (option string))
    "single max" (Some "com.example/mmm") max_single ;
  (* multiple entries at different layers *)
  let%lwt mst = Mem_mst.add mst "com.example/aaa" cid1 in
  let%lwt mst = Mem_mst.add mst "com.example/zzz" cid1 in
  let%lwt mst = Mem_mst.add mst "com.example/bbb" cid1 in
  let%lwt mst = Mem_mst.add mst "com.example/yyy" cid1 in
  let%lwt min_key = Mem_mst.get_min_key mst mst.root in
  let%lwt max_key = Mem_mst.get_max_key mst mst.root in
  Alcotest.(check (option string)) "multi min" (Some "com.example/aaa") min_key ;
  Alcotest.(check (option string)) "multi max" (Some "com.example/zzz") max_key ;
  (* add keys with high layer values to exercise deeper tree structure *)
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fs2j" cid1 in
  let%lwt mst = Mem_mst.add mst "com.example.record/3jqfcqzm3fn2j" cid1 in
  let%lwt min_deep = Mem_mst.get_min_key mst mst.root in
  let%lwt max_deep = Mem_mst.get_max_key mst mst.root in
  Alcotest.(check (option string))
    "deep min" (Some "com.example.record/3jqfcqzm3fn2j") min_deep ;
  Alcotest.(check (option string)) "deep max" (Some "com.example/zzz") max_deep ;
  Lwt.return_ok ()

let () =
  let open Alcotest in
  let run_test test =
    match Lwt_main.run (test ()) with
    | Ok () ->
        ()
    | Error e ->
        Alcotest.fail (Printexc.to_string e)
  in
  run "mst"
    [ ( "basic ops"
      , [ test_case "adds records" `Quick (fun () -> run_test test_adds)
        ; test_case "edits records" `Quick (fun () -> run_test test_edits)
        ; test_case "deletes records" `Quick (fun () -> run_test test_deletes)
        ; test_case "order independent" `Quick (fun () ->
              run_test test_order_independent )
        ; test_case "saves and loads" `Quick (fun () ->
              run_test test_save_load ) ] )
    ; ( "mst roundtrip"
      , [ test_case "car→mst→car→mst roundtrip" `Quick (fun () ->
              Lwt_main.run (test_roundtrip ()) ) ] )
    ; ( "allowable keys"
      , [ test_case "allowed mst keys" `Quick (fun () ->
              run_test test_allowable_keys ) ] )
    ; ("diffs", [test_case "diffs" `Quick (fun () -> run_test test_diffs)])
    ; ( "interop edge cases"
      , [ test_case "trims top of tree on delete" `Quick (fun () ->
              run_test test_trims_top_on_delete )
        ; test_case "insertion splits two layers down" `Quick (fun () ->
              run_test test_insertion_splits_two_layers_down )
        ; test_case "new layers two higher than existing" `Quick (fun () ->
              run_test test_new_layers_two_higher_than_existing ) ] )
    ; ( "interop known maps"
      , [ test_case "computes empty tree root cid" `Quick (fun () ->
              run_test test_empty_root )
        ; test_case "computes trivial tree root cid" `Quick (fun () ->
              run_test test_trivial_root )
        ; test_case "computes singlelayer2 tree root cid" `Quick (fun () ->
              run_test test_singlelayer2_root )
        ; test_case "computes simple tree root cid" `Quick (fun () ->
              run_test test_simple_root ) ] )
    ; ( "incremental canonicity"
      , [ test_case "add_incremental matches add" `Quick (fun () ->
              run_test test_incremental_add_canonicity )
        ; test_case "delete_incremental matches delete" `Quick (fun () ->
              run_test test_incremental_delete_canonicity )
        ; test_case "mixed incremental ops" `Quick (fun () ->
              run_test test_incremental_mixed_ops_canonicity )
        ; test_case "incremental edge cases" `Quick (fun () ->
              run_test test_incremental_edge_cases ) ] )
    ; ( "boundary functions"
      , [ test_case "get_min_key and get_max_key" `Quick (fun () ->
              run_test test_get_min_max_keys ) ] ) ]
