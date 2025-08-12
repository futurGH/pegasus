open Mist
open Lwt.Infix
module MemMst = Mst.Make (Storage.Memory_blockstore)
module StringMap = Dag_cbor.StringMap

let cid_of_string_exn s =
  match Cid.of_string s with Ok c -> c | Error msg -> failwith msg

module Keys = struct
  let a0 = "A0/501344"

  let a2 = "A2/239654"

  let b0 = "B0/436099"

  let b1 = "B1/293486"

  let b2 = "B2/303249"

  let c0 = "C0/535043"

  let c2 = "C2/953910"

  let d0 = "D0/360671"

  let d2 = "D2/915466"

  let e0 = "E0/922708"

  let e2 = "E2/413113"

  let f0 = "F0/606463"

  let f1 = "F1/415452"

  let g0 = "G0/714257"

  let g2 = "G2/536869"

  let h0 = "H0/740256"
end

let leaf_cid =
  cid_of_string_exn
    "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"

let mst_of_proof root proof : MemMst.t =
  let store = Storage.Memory_blockstore.create ~blocks:proof () in
  MemMst.create store root

let test_two_deep_split () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mst = MemMst.add mst Keys.a0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.b1 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.c0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.e0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.f1 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.g0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.d2 leaf_cid in
  let%lwt proof = MemMst.get_covering_proof mst Keys.d2 in
  let proof_mst = mst_of_proof mst.root proof in
  let%lwt got = MemMst.get_cid proof_mst Keys.d2 in
  Alcotest.(check bool)
    "covering proof proves d2" true
    (Option.value
       (Option.map (fun x -> Cid.equal leaf_cid x) got)
       ~default:false ) ;
  Lwt.return_unit

let test_two_deep_leafless_splits () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mst = MemMst.add mst Keys.a0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.b0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.d0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.e0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.c2 leaf_cid in
  let%lwt proof = MemMst.get_covering_proof mst Keys.c2 in
  let proof_mst = mst_of_proof mst.root proof in
  let%lwt got = MemMst.get_cid proof_mst Keys.c2 in
  Alcotest.(check bool)
    "covering proof proves c2" true
    (Option.value
       (Option.map (fun x -> Cid.equal leaf_cid x) got)
       ~default:false ) ;
  Lwt.return_unit

let test_add_on_edge_with_neighbor_two_layers_down () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mst = MemMst.add mst Keys.a0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.b2 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.c0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.d2 leaf_cid in
  let%lwt proof = MemMst.get_covering_proof mst Keys.d2 in
  let proof_mst = mst_of_proof mst.root proof in
  let%lwt got = MemMst.get_cid proof_mst Keys.d2 in
  Alcotest.(check bool)
    "covering proof proves d2" true
    (Option.value
       (Option.map (fun x -> Cid.equal leaf_cid x) got)
       ~default:false ) ;
  Lwt.return_unit

let test_merge_and_split_in_multi_op_commit () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mst = MemMst.add mst Keys.b0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.c2 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.d0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.e2 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.f0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.h0 leaf_cid in
  let%lwt mst = MemMst.delete mst Keys.b2 in
  let%lwt mst = MemMst.delete mst Keys.d2 in
  let%lwt mst = MemMst.add mst Keys.c2 leaf_cid in
  let%lwt proofs =
    Lwt.all
      [ MemMst.get_covering_proof mst Keys.b2
      ; MemMst.get_covering_proof mst Keys.d2
      ; MemMst.get_covering_proof mst Keys.c2 ]
  in
  let proof =
    List.fold_left Storage.Block_map.merge Storage.Block_map.empty proofs
  in
  let proof_mst = mst_of_proof mst.root proof in
  let%lwt got_c2 = MemMst.get_cid proof_mst Keys.c2 in
  Alcotest.(check bool)
    "covering proof proves c2" true
    (Option.value
       (Option.map (fun x -> Cid.equal leaf_cid x) got_c2)
       ~default:false ) ;
  let%lwt got_b2 = MemMst.get_cid proof_mst Keys.b2 in
  Alcotest.(check bool)
    "covering proof proves non-membership of b2" true (got_b2 = None) ;
  let%lwt got_d2 = MemMst.get_cid proof_mst Keys.d2 in
  Alcotest.(check bool)
    "covering proof proves non-membership of d2" true (got_d2 = None) ;
  Lwt.return_unit

let test_complex_multi_op_commit () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mst = MemMst.add mst Keys.b0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.c2 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.d0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.e2 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.f0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.h0 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.a2 leaf_cid in
  let%lwt mst = MemMst.add mst Keys.g2 leaf_cid in
  let%lwt mst = MemMst.delete mst Keys.c2 in
  let%lwt proofs =
    Lwt.all
      [ MemMst.get_covering_proof mst Keys.a2
      ; MemMst.get_covering_proof mst Keys.g2
      ; MemMst.get_covering_proof mst Keys.c2 ]
  in
  let proof =
    List.fold_left Storage.Block_map.merge Storage.Block_map.empty proofs
  in
  let proof_mst = mst_of_proof mst.root proof in
  let%lwt got_a2 = MemMst.get_cid proof_mst Keys.a2 in
  Alcotest.(check bool)
    "covering proof proves a2" true
    (Option.value
       (Option.map (fun x -> Cid.equal leaf_cid x) got_a2)
       ~default:false ) ;
  let%lwt got_g2 = MemMst.get_cid proof_mst Keys.g2 in
  Alcotest.(check bool)
    "covering proof proves g2" true
    (Option.value
       (Option.map (fun x -> Cid.equal leaf_cid x) got_g2)
       ~default:false ) ;
  let%lwt got_c2 = MemMst.get_cid proof_mst Keys.c2 in
  Alcotest.(check bool)
    "covering proof proves non-membership of c2" true (got_c2 = None) ;
  Lwt.return_unit

let test_trims_top_on_delete () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let l1root = "bafyreifnqrwbk6ffmyaz5qtujqrzf5qmxf7cbxvgzktl4e3gabuxbtatv4" in
  let l0root = "bafyreie4kjuxbwkhzg2i5dljaswcroeih4dgiqq6pazcmunwt2byd725vi" in
  let%lwt mst = MemMst.create_empty store in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fn2j" cid1 in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fo2j" cid1 in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fp2j" cid1 in
  (* level 1 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fs2j" cid1 in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3ft2j" cid1 in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fu2j" cid1 in
  let%lwt cnt = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (before delete)" 6 cnt ;
  let%lwt layer_before = MemMst.layer mst in
  Alcotest.(check int) "root layer before delete" 1 layer_before ;
  let root_before = mst.root in
  Alcotest.(check string)
    "root cid before delete" l1root
    (Cid.to_string root_before) ;
  (* delete level 1 entry, expect trimming to layer 0 *)
  let%lwt mst' = MemMst.delete mst "com.example.record/3jqfcqzm3fs2j" in
  let%lwt cnt' = MemMst.leaf_count mst' in
  Alcotest.(check int) "leaf count (after delete)" 5 cnt' ;
  let%lwt layer_after = MemMst.layer mst' in
  Alcotest.(check int) "root layer after delete" 0 layer_after ;
  let root_after = mst'.root in
  Alcotest.(check string)
    "root cid after delete" l0root (Cid.to_string root_after) ;
  Lwt.return_unit

let test_insertion_splits_two_layers_down () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let l1root = "bafyreiettyludka6fpgp33stwxfuwhkzlur6chs4d2v4nkmq2j3ogpdjem" in
  let l2root = "bafyreid2x5eqs4w4qxvc5jiwda4cien3gw2q6cshofxwnvv7iucrmfohpm" in
  let%lwt mst = MemMst.create_empty store in
  (* A; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fo2j" cid1 in
  (* B; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fp2j" cid1 in
  (* C; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fr2j" cid1 in
  (* D; level 1 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fs2j" cid1 in
  (* E; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3ft2j" cid1 in
  (* G; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fz2j" cid1 in
  (* H; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm4fc2j" cid1 in
  (* I; level 1 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm4fd2j" cid1 in
  (* J; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm4ff2j" cid1 in
  (* K; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm4fg2j" cid1 in
  (* L; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm4fh2j" cid1 in
  let%lwt cnt = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (before F)" 11 cnt ;
  let%lwt layer_before = MemMst.layer mst in
  Alcotest.(check int) "root layer (before F)" 1 layer_before ;
  let root_before = mst.root in
  Alcotest.(check string)
    "root cid (before F)" l1root
    (Cid.to_string root_before) ;
  (* insert F; level 2 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fx2j" cid1 in
  let%lwt cnt_after_f = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (after F)" 12 cnt_after_f ;
  let%lwt layer_after_f = MemMst.layer mst in
  Alcotest.(check int) "root layer (after F)" 2 layer_after_f ;
  let root_after_f = mst.root in
  Alcotest.(check string)
    "root cid (after F)" l2root
    (Cid.to_string root_after_f) ;
  (* remove F; should return to previous root/layer *)
  let%lwt mst = MemMst.delete mst "com.example.record/3jqfcqzm3fx2j" in
  let%lwt cnt_after_del_f = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (after del F)" 11 cnt_after_del_f ;
  let%lwt layer_after_del_f = MemMst.layer mst in
  Alcotest.(check int) "root layer (after del F)" 1 layer_after_del_f ;
  let root_after_del_f = mst.root in
  Alcotest.(check string)
    "root cid (after del F)" l1root
    (Cid.to_string root_after_del_f) ;
  Lwt.return_unit

let test_new_layers_two_higher_than_existing () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let l0root = "bafyreidfcktqnfmykz2ps3dbul35pepleq7kvv526g47xahuz3rqtptmky" in
  let l2root = "bafyreiavxaxdz7o7rbvr3zg2liox2yww46t7g6hkehx4i4h3lwudly7dhy" in
  let l2root2 = "bafyreig4jv3vuajbsybhyvb7gggvpwh2zszwfyttjrj6qwvcsp24h6popu" in
  let%lwt mst = MemMst.create_empty store in
  (* A; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3ft2j" cid1 in
  (* C; level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fz2j" cid1 in
  let%lwt cnt = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,C)" 2 cnt ;
  let%lwt layer_ac = MemMst.layer mst in
  Alcotest.(check int) "root layer (A,C)" 0 layer_ac ;
  let root_ac = mst.root in
  Alcotest.(check string) "root cid (A,C)" l0root (Cid.to_string root_ac) ;
  (* insert B (level 2) *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fx2j" cid1 in
  let%lwt cnt_abc = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,B,C)" 3 cnt_abc ;
  let%lwt layer_abc = MemMst.layer mst in
  Alcotest.(check int) "root layer (A,B,C)" 2 layer_abc ;
  let root_abc = mst.root in
  Alcotest.(check string) "root cid (A,B,C)" l2root (Cid.to_string root_abc) ;
  (* remove B → back to l0root *)
  let%lwt mst = MemMst.delete mst "com.example.record/3jqfcqzm3fx2j" in
  let%lwt cnt_ac = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,C again)" 2 cnt_ac ;
  let%lwt layer_ac2 = MemMst.layer mst in
  Alcotest.(check int) "root layer (A,C again)" 0 layer_ac2 ;
  let root_ac2 = mst.root in
  Alcotest.(check string) "root cid (A,C again)" l0root (Cid.to_string root_ac2) ;
  (* insert B (level 2) and D (level 1) *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fx2j" cid1 in
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm4fd2j" cid1 in
  let%lwt cnt_abcd = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,B,C,D)" 4 cnt_abcd ;
  let%lwt layer_abcd = MemMst.layer mst in
  Alcotest.(check int) "root layer (A,B,C,D)" 2 layer_abcd ;
  let root_abcd = mst.root in
  Alcotest.(check string) "root cid (A,B,C,D)" l2root2 (Cid.to_string root_abcd) ;
  (* remove D → match l2root *)
  let%lwt mst = MemMst.delete mst "com.example.record/3jqfcqzm4fd2j" in
  let%lwt cnt_abc2 = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (A,B,C again)" 3 cnt_abc2 ;
  let%lwt layer_abc2 = MemMst.layer mst in
  Alcotest.(check int) "root layer (A,B,C again)" 2 layer_abc2 ;
  let root_abc2 = mst.root in
  Alcotest.(check string)
    "root cid (A,B,C again)" l2root (Cid.to_string root_abc2) ;
  Lwt.return_unit

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
  Storage.Memory_blockstore.put_block store cid bytes >|= fun () -> cid

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
    (fun acc k -> put_random_block store >|= fun cid -> StringMap.add k cid acc)
    StringMap.empty keys

let shuffle lst =
  let arr = Array.of_list lst in
  for i = Array.length arr - 1 downto 1 do
    let j = Random.int (i + 1) in
    let tmp = arr.(i) in
    arr.(i) <- arr.(j) ;
    arr.(j) <- tmp
  done ;
  Array.to_list arr

let assoc_of_map m = StringMap.bindings m

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
  let%lwt mst = MemMst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst' =
    Lwt_list.fold_left_s (fun t (k, v) -> MemMst.add t k v) mst shuffled
  in
  let%lwt () =
    Lwt_list.iter_s
      (fun (k, v) ->
        let%lwt got = MemMst.get_cid mst' k in
        Alcotest.(check bool)
          "added records retrievable" true
          (Option.value
             (Option.map (fun x -> Cid.equal v x) got)
             ~default:false )
        |> Lwt.return )
      shuffled
  in
  let%lwt total = MemMst.leaf_count mst' in
  Alcotest.(check int) "leaf count after adds" 1000 total ;
  Lwt.return_unit

let test_edits () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> MemMst.add t k v) mst shuffled
  in
  let to_edit = take 100 shuffled in
  let%lwt edited_mst, edited =
    Lwt_list.fold_left_s
      (fun (t, acc) (k, _old) ->
        let%lwt new_cid = put_random_block store in
        MemMst.add t k new_cid >|= fun t' -> (t', (k, new_cid) :: acc) )
      (mst, []) to_edit
  in
  let edited = List.rev edited in
  let%lwt () =
    Lwt_list.iter_s
      (fun (k, v) ->
        let%lwt got = MemMst.get_cid edited_mst k in
        Alcotest.(check bool)
          "updated records retrievable" true
          (Option.value
             (Option.map (fun x -> Cid.equal v x) got)
             ~default:false )
        |> Lwt.return )
      edited
  in
  let%lwt total = MemMst.leaf_count edited_mst in
  Alcotest.(check int) "leaf count stable after edits" 1000 total ;
  Lwt.return_unit

let test_deletes () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> MemMst.add t k v) mst shuffled
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
    Lwt_list.fold_left_s (fun t (k, _) -> MemMst.delete t k) mst to_delete
  in
  let%lwt total = MemMst.leaf_count deleted_mst in
  Alcotest.(check int) "leaf count after deletes" 900 total ;
  let%lwt () =
    Lwt_list.iter_s
      (fun (k, _) ->
        let%lwt got = MemMst.get_cid deleted_mst k in
        Alcotest.(check bool) "deleted record missing" true (got = None)
        |> Lwt.return )
      to_delete
  in
  let%lwt () =
    Lwt_list.iter_s
      (fun (k, v) ->
        let%lwt got = MemMst.get_cid deleted_mst k in
        Alcotest.(check bool)
          "remaining records intact" true
          (Option.value
             (Option.map (fun x -> Cid.equal v x) got)
             ~default:false )
        |> Lwt.return )
      the_rest
  in
  Lwt.return_unit

let test_order_independent () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> MemMst.add t k v) mst shuffled
  in
  let%lwt all_nodes = MemMst.all_nodes mst in
  let%lwt recreated = MemMst.create_empty store in
  let reshuffled = shuffle (assoc_of_map mapping) in
  let%lwt recreated =
    Lwt_list.fold_left_s (fun t (k, v) -> MemMst.add t k v) recreated reshuffled
  in
  let%lwt all_reshuffled = MemMst.all_nodes recreated in
  Alcotest.(check int)
    "node count equal" (List.length all_nodes)
    (List.length all_reshuffled) ;
  List.iter2
    (fun (cid1, bytes1) (cid2, bytes2) ->
      Alcotest.(check bool) "cid equal" true (Cid.equal cid1 cid2) ;
      Alcotest.(check string)
        "bytes equal" (Bytes.to_string bytes1) (Bytes.to_string bytes2) )
    all_nodes all_reshuffled ;
  Lwt.return_unit

let test_save_load () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 300 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> MemMst.add t k v) mst shuffled
  in
  let loaded = MemMst.create store mst.root in
  let%lwt orig_nodes = MemMst.all_nodes mst in
  let%lwt loaded_nodes = MemMst.all_nodes loaded in
  Alcotest.(check int)
    "node count equal" (List.length orig_nodes) (List.length loaded_nodes) ;
  List.iter2
    (fun (cid1, bytes1) (cid2, bytes2) ->
      Alcotest.(check bool) "cid equal" true (Cid.equal cid1 cid2) ;
      Alcotest.(check string)
        "bytes equal" (Bytes.to_string bytes1) (Bytes.to_string bytes2) )
    orig_nodes loaded_nodes ;
  Lwt.return_unit

let test_diffs () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst0 = MemMst.create_empty store in
  let%lwt mapping = generate_bulk_data_keys store 1000 in
  let shuffled = shuffle (assoc_of_map mapping) in
  let%lwt mst =
    Lwt_list.fold_left_s (fun t (k, v) -> MemMst.add t k v) mst0 shuffled
  in
  (* additions *)
  let%lwt add_map = generate_bulk_data_keys store 100 in
  let to_add = assoc_of_map add_map in
  (* edits from existing *)
  let to_edit = take 100 (drop 500 shuffled) in
  (* deletes from existing *)
  let to_del = take 100 (drop 400 shuffled) in
  let expected_adds =
    List.fold_left (fun m (k, v) -> StringMap.add k v m) StringMap.empty to_add
  in
  let%lwt to_diff, expected_updates =
    Lwt_list.fold_left_s
      (fun (t, m) (k, old_v) ->
        let%lwt updated = put_random_block store in
        let m' = StringMap.add k (old_v, updated) m in
        MemMst.add t k updated >|= fun t' -> (t', m') )
      (mst, StringMap.empty) to_edit
  in
  let%lwt to_diff, expected_dels =
    Lwt_list.fold_left_s
      (fun (t, m) (k, v) ->
        MemMst.delete t k >|= fun t' -> (t', StringMap.add k v m) )
      (to_diff, StringMap.empty) to_del
  in
  let%lwt to_diff =
    Lwt_list.fold_left_s (fun t (k, v) -> MemMst.add t k v) to_diff to_add
  in
  let%lwt diff = MemMst.mst_diff to_diff (Some mst) in
  (* lengths *)
  Alcotest.(check int) "adds length" 100 (List.length diff.adds) ;
  Alcotest.(check int) "updates length" 100 (List.length diff.updates) ;
  Alcotest.(check int) "deletes length" 100 (List.length diff.deletes) ;
  (* contents: convert to maps to compare *)
  let adds_map =
    List.fold_left
      (fun m (a : MemMst.diff_add) -> StringMap.add a.key a.cid m)
      StringMap.empty diff.adds
  in
  let updates_map =
    List.fold_left
      (fun m (u : MemMst.diff_update) -> StringMap.add u.key (u.prev, u.cid) m)
      StringMap.empty diff.updates
  in
  let deletes_map =
    List.fold_left
      (fun m (d : MemMst.diff_delete) -> StringMap.add d.key d.cid m)
      StringMap.empty diff.deletes
  in
  (* compare adds *)
  Alcotest.(check int)
    "adds map size equal"
    (StringMap.cardinal expected_adds)
    (StringMap.cardinal adds_map) ;
  StringMap.iter
    (fun k v ->
      match StringMap.find_opt k adds_map with
      | Some v' ->
          Alcotest.(check bool) "add cid equal" true (Cid.equal v v')
      | None ->
          Alcotest.failf "missing add key %s" k )
    expected_adds ;
  (* compare updates *)
  Alcotest.(check int)
    "updates map size equal"
    (StringMap.cardinal expected_updates)
    (StringMap.cardinal updates_map) ;
  StringMap.iter
    (fun k (prev, cid) ->
      match StringMap.find_opt k updates_map with
      | Some (prev', cid') ->
          Alcotest.(check bool) "update prev equal" true (Cid.equal prev prev') ;
          Alcotest.(check bool) "update cid equal" true (Cid.equal cid cid')
      | None ->
          Alcotest.failf "missing update key %s" k )
    expected_updates ;
  (* compare deletes *)
  Alcotest.(check int)
    "deletes map size equal"
    (StringMap.cardinal expected_dels)
    (StringMap.cardinal deletes_map) ;
  StringMap.iter
    (fun k v ->
      match StringMap.find_opt k deletes_map with
      | Some v' ->
          Alcotest.(check bool) "delete cid equal" true (Cid.equal v v')
      | None ->
          Alcotest.failf "missing delete key %s" k )
    expected_dels ;
  (* ensure we correctly report all added CIDs *)
  let%lwt leaves = MemMst.leaves_of_root to_diff in
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
  Lwt.return_unit

let test_allowable_keys () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let cid1 =
    Cid.of_string "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
    |> Result.get_ok
  in
  let expect_reject key =
    Lwt.catch
      (fun () ->
        MemMst.add mst key cid1
        >>= fun _ ->
        Alcotest.failf "expected invalid key to be rejected: %s" key )
      (function
        | Invalid_argument _ ->
            Lwt.return_unit
        | exn ->
            Alcotest.failf "unexpected exception for %s: %s" key
              (Printexc.to_string exn) )
  in
  let expect_allow key = MemMst.add mst key cid1 >|= fun _ -> () in
  let%lwt () = expect_reject "" in
  let%lwt () = expect_reject "asdf" in
  let%lwt () = expect_reject "nested/collection/asdf" in
  let%lwt () = expect_reject "coll/" in
  let%lwt () = expect_reject "/rkey" in
  let%lwt () = expect_reject "coll/jalapeñoA" in
  let%lwt () = expect_reject "coll/coöperative" in
  let%lwt () = expect_reject "coll/abc💩" in
  let%lwt () = expect_reject "coll/key$" in
  let%lwt () = expect_reject "coll/key%" in
  let%lwt () = expect_reject "coll/key(" in
  let%lwt () = expect_reject "coll/key)" in
  let%lwt () = expect_reject "coll/key+" in
  let%lwt () = expect_reject "coll/key=" in
  let%lwt () = expect_reject "coll/@handle" in
  let%lwt () = expect_reject "coll/any space" in
  let%lwt () = expect_reject "coll/#extra" in
  let%lwt () = expect_reject "coll/any+space" in
  let%lwt () = expect_reject "coll/number[3]" in
  let%lwt () = expect_reject "coll/number(3)" in
  let%lwt () = expect_reject "coll/dHJ1ZQ==" in
  let%lwt () = expect_reject "coll/\"quote\"" in
  let big =
    "coll/"
    ^ String.concat ""
        (Array.to_list
           (Array.init 1100 (fun _ ->
                String.make 1 (Char.chr (97 + Random.int 26)) ) ) )
  in
  let%lwt () = expect_reject big in
  let%lwt () = expect_allow "coll/3jui7kd54zh2y" in
  let%lwt () = expect_allow "coll/self" in
  let%lwt () = expect_allow "coll/example.com" in
  let%lwt () = expect_allow "com.example/rkey" in
  let%lwt () = expect_allow "coll/~1.2-3_" in
  let%lwt () = expect_allow "coll/dHJ1ZQ" in
  let%lwt () = expect_allow "coll/pre:fix" in
  let%lwt () = expect_allow "coll/_" in
  Lwt.return_unit

let test_empty_root () =
  let store = Storage.Memory_blockstore.create () in
  let%lwt mst = MemMst.create_empty store in
  let%lwt cnt = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (empty)" 0 cnt ;
  Alcotest.(check string)
    "empty root cid"
    "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"
    (Cid.to_string mst.root) ;
  Lwt.return_unit

let test_trivial_root () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fo2j" cid1 in
  let%lwt cnt = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (trivial)" 1 cnt ;
  Alcotest.(check string)
    "trivial root cid"
    "bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu"
    (Cid.to_string mst.root) ;
  Lwt.return_unit

let test_singlelayer2_root () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let%lwt mst = MemMst.create_empty store in
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fx2j" cid1 in
  let%lwt cnt = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (singlelayer2)" 1 cnt ;
  let%lwt layer = MemMst.layer mst in
  Alcotest.(check int) "root layer (singlelayer2)" 2 layer ;
  Alcotest.(check string)
    "singlelayer2 root cid"
    "bafyreih7wfei65pxzhauoibu3ls7jgmkju4bspy4t2ha2qdjnzqvoy33ai"
    (Cid.to_string mst.root) ;
  Lwt.return_unit

let test_simple_root () =
  let store = Storage.Memory_blockstore.create () in
  let cid1 =
    cid_of_string_exn
      "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
  in
  let%lwt mst = MemMst.create_empty store in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fp2j" cid1 in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fr2j" cid1 in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3fs2j" cid1 in
  (* level 1 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm3ft2j" cid1 in
  (* level 0 *)
  let%lwt mst = MemMst.add mst "com.example.record/3jqfcqzm4fc2j" cid1 in
  (* level 0 *)
  let%lwt cnt = MemMst.leaf_count mst in
  Alcotest.(check int) "leaf count (simple)" 5 cnt ;
  Alcotest.(check string)
    "simple root cid"
    "bafyreicmahysq4n6wfuxo522m6dpiy7z7qzym3dzs756t5n7nfdgccwq7m"
    (Cid.to_string mst.root) ;
  Lwt.return_unit

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
    let%lwt bm =
      Lwt_seq.fold_left
        (fun acc (cid, bytes) -> Storage.Block_map.set cid bytes acc)
        Storage.Block_map.empty blocks
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
      MemMst.create store
        ( match StringMap.find "data" commit with
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
  let mst_stream = MemMst.to_blocks_stream mst in
  let commit_bytes = Dag_cbor.encode (`Map commit) in
  let commit_cid = Cid.create Dcbor commit_bytes in
  let%lwt car' =
    Car.blocks_to_car (Some commit_cid)
      (Lwt_seq.append (Lwt_seq.of_list [(commit_cid, commit_bytes)]) mst_stream)
  in
  let%lwt _, mst' = mst_of_car_bytes car' in
  let%lwt eq = MemMst.equal mst mst' in
  Lwt.return (Alcotest.(check bool) "mst roundtrip" true eq)

let () =
  let open Alcotest in
  run "mst"
    [ ( "basic ops"
      , [ test_case "adds records" `Quick (fun () ->
              Lwt_main.run (test_adds ()) )
        ; test_case "edits records" `Quick (fun () ->
              Lwt_main.run (test_edits ()) )
        ; test_case "deletes records" `Quick (fun () ->
              Lwt_main.run (test_deletes ()) )
        ; test_case "order independent" `Quick (fun () ->
              Lwt_main.run (test_order_independent ()) )
        ; test_case "saves and loads" `Quick (fun () ->
              Lwt_main.run (test_save_load ()) ) ] )
    ; ( "mst roundtrip"
      , [ test_case "car→mst→car→mst roundtrip" `Quick (fun () ->
              Lwt_main.run (test_roundtrip ()) ) ] )
    ; ( "allowable keys"
      , [ test_case "allowed mst keys" `Quick (fun () ->
              Lwt_main.run (test_allowable_keys ()) ) ] )
    ; ( "diffs"
      , [test_case "diffs" `Quick (fun () -> Lwt_main.run (test_diffs ()))] )
    ; ( "covering-proofs"
      , [ test_case "two deep split" `Quick (fun () ->
              Lwt_main.run (test_two_deep_split ()) )
        ; test_case "two deep leafless splits" `Quick (fun () ->
              Lwt_main.run (test_two_deep_leafless_splits ()) )
        ; test_case "edge with neighbour two layers down" `Quick (fun () ->
              Lwt_main.run (test_add_on_edge_with_neighbor_two_layers_down ()) )
        ; test_case "merge and split in multi-op commit" `Quick (fun () ->
              Lwt_main.run (test_merge_and_split_in_multi_op_commit ()) )
        ; test_case "complex multi-op commit" `Quick (fun () ->
              Lwt_main.run (test_complex_multi_op_commit ()) ) ] )
    ; ( "interop edge cases"
      , [ test_case "trims top of tree on delete" `Quick (fun () ->
              Lwt_main.run (test_trims_top_on_delete ()) )
        ; test_case "insertion splits two layers down" `Quick (fun () ->
              Lwt_main.run (test_insertion_splits_two_layers_down ()) )
        ; test_case "new layers two higher than existing" `Quick (fun () ->
              Lwt_main.run (test_new_layers_two_higher_than_existing ()) ) ] )
    ; ( "interop known maps"
      , [ test_case "computes empty tree root cid" `Quick (fun () ->
              Lwt_main.run (test_empty_root ()) )
        ; test_case "computes trivial tree root cid" `Quick (fun () ->
              Lwt_main.run (test_trivial_root ()) )
        ; test_case "computes singlelayer2 tree root cid" `Quick (fun () ->
              Lwt_main.run (test_singlelayer2_root ()) )
        ; test_case "computes simple tree root cid" `Quick (fun () ->
              Lwt_main.run (test_simple_root ()) ) ] ) ]
