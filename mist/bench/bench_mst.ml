open Mist
open Lwt.Infix
module Mem_mst = Mst.Make (Storage.Memory_blockstore)
module String_map = Dag_cbor.String_map

type timing_result =
  {name: string; iterations: int; total_s: float; per_iter_s: float}

let time_it name f : timing_result Lwt.t =
  let start = Unix.gettimeofday () in
  let%lwt _ = f () in
  let elapsed = Unix.gettimeofday () -. start in
  Lwt.return {name; iterations= 1; total_s= elapsed; per_iter_s= elapsed}

let time_it_n name n f : timing_result Lwt.t =
  let start = Unix.gettimeofday () in
  let%lwt () =
    let rec loop i =
      if i >= n then Lwt.return_unit else f () >>= fun _ -> loop (i + 1)
    in
    loop 0
  in
  let elapsed = Unix.gettimeofday () -. start in
  Lwt.return
    { name
    ; iterations= n
    ; total_s= elapsed
    ; per_iter_s= elapsed /. float_of_int n }

let print_result r =
  if r.iterations = 1 then Printf.printf "  %-50s %10.4f s\n%!" r.name r.total_s
  else
    Printf.printf "  %-50s %10.4f s total, %10.6f s/iter (%d iters)\n%!" r.name
      r.total_s r.per_iter_s r.iterations

let print_header name = Printf.printf "\n=== %s ===\n%!" name

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

let generate_bulk_data store count =
  let keys = unique_keys count [] in
  Lwt_list.map_s (fun k -> put_random_block store >|= fun cid -> (k, cid)) keys

let shuffle lst =
  let arr = Array.of_list lst in
  for i = Array.length arr - 1 downto 1 do
    let j = Random.int (i + 1) in
    let tmp = arr.(i) in
    arr.(i) <- arr.(j) ;
    arr.(j) <- tmp
  done ;
  Array.to_list arr

let bench_of_assoc sizes =
  print_header "bulk creation" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt r =
        time_it (Printf.sprintf "of_assoc %d records" size) (fun () ->
            Mem_mst.of_assoc store data )
      in
      print_result r ; Lwt.return_unit )
    sizes

let bench_incremental_add sizes =
  print_header "incremental add vs add_rebuild" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let shuffled = shuffle data in
      (* incremental add *)
      let%lwt mst_base =
        match%lwt Mem_mst.create_empty store with
        | Ok mst ->
            Lwt.return mst
        | Error e ->
            raise e
      in
      let%lwt r1 =
        time_it (Printf.sprintf "add (incremental) %d records" size) (fun () ->
            Lwt_list.fold_left_s
              (fun t (k, v) -> Mem_mst.add t k v)
              mst_base shuffled )
      in
      print_result r1 ;
      (* add_rebuild for comparison *)
      let%lwt mst_base2 =
        match%lwt Mem_mst.create_empty store with
        | Ok mst ->
            Lwt.return mst
        | Error e ->
            raise e
      in
      let%lwt r2 =
        time_it (Printf.sprintf "add_rebuild %d records" size) (fun () ->
            Lwt_list.fold_left_s
              (fun t (k, v) -> Mem_mst.add_rebuild t k v)
              mst_base2 shuffled )
      in
      print_result r2 ;
      let speedup = r2.total_s /. r1.total_s in
      Printf.printf "  -> incremental is %.2fx %s\n%!" (abs_float speedup)
        (if speedup > 1.0 then "faster" else "slower") ;
      Lwt.return_unit )
    sizes

let bench_incremental_delete sizes =
  print_header "incremental delete vs delete_rebuild" ;
  Lwt_list.iter_s
    (fun (tree_size, delete_count) ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store tree_size in
      let%lwt mst = Mem_mst.of_assoc store data in
      let to_delete =
        shuffle data |> List.filteri (fun i _ -> i < delete_count)
      in
      (* incremental delete *)
      let%lwt r1 =
        time_it
          (Printf.sprintf "delete (incr) %d from %d" delete_count tree_size)
          (fun () ->
            Lwt_list.fold_left_s
              (fun t (k, _) -> Mem_mst.delete t k)
              mst to_delete )
      in
      print_result r1 ;
      (* rebuild the tree for delete_rebuild test *)
      let%lwt mst2 = Mem_mst.of_assoc store data in
      let%lwt r2 =
        time_it
          (Printf.sprintf "delete_rebuild %d from %d" delete_count tree_size)
          (fun () ->
            Lwt_list.fold_left_s
              (fun t (k, _) -> Mem_mst.delete_rebuild t k)
              mst2 to_delete )
      in
      print_result r2 ;
      let speedup = r2.total_s /. r1.total_s in
      Printf.printf "  -> incremental is %.2fx %s\n%!" (abs_float speedup)
        (if speedup > 1.0 then "faster" else "slower") ;
      Lwt.return_unit )
    sizes

let bench_single_add_scaling sizes =
  print_header "time to add 1 record to tree of size n" ;
  let iterations = 500 in
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst = Mem_mst.of_assoc store data in
      let%lwt extra_data = generate_bulk_data store iterations in
      let%lwt r =
        time_it_n (Printf.sprintf "single add to %d-record tree" size)
          iterations (fun () ->
            let k, v = List.nth extra_data (Random.int iterations) in
            Mem_mst.add mst k v >|= fun _ -> () )
      in
      print_result r ; Lwt.return_unit )
    sizes

let bench_single_delete_scaling sizes =
  print_header "time to delete 1 record from tree of size n" ;
  let iterations = 500 in
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst = Mem_mst.of_assoc store data in
      let shuffled = shuffle data in
      let idx = ref 0 in
      let%lwt r =
        time_it_n (Printf.sprintf "single delete from %d-record tree" size)
          (min iterations size) (fun () ->
            let k, _ = List.nth shuffled !idx in
            idx := !idx + 1 ;
            Mem_mst.delete mst k >|= fun _ -> () )
      in
      print_result r ; Lwt.return_unit )
    sizes

let bench_traversal sizes =
  print_header "traversal" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst = Mem_mst.of_assoc store data in
      let%lwt r1 =
        time_it (Printf.sprintf "build_map %d records" size) (fun () ->
            Mem_mst.build_map mst )
      in
      print_result r1 ;
      let%lwt r2 =
        time_it (Printf.sprintf "leaves_of_root %d records" size) (fun () ->
            Mem_mst.leaves_of_root mst )
      in
      print_result r2 ;
      let%lwt r3 =
        time_it (Printf.sprintf "leaf_count %d records" size) (fun () ->
            Mem_mst.leaf_count mst )
      in
      print_result r3 ;
      let%lwt r4 =
        time_it (Printf.sprintf "all_nodes %d records" size) (fun () ->
            Mem_mst.all_nodes mst )
      in
      print_result r4 ;
      let%lwt r5 =
        time_it (Printf.sprintf "collect_nodes_and_leaves %d records" size)
          (fun () -> Mem_mst.collect_nodes_and_leaves mst )
      in
      print_result r5 ; Lwt.return_unit )
    sizes

let bench_streaming sizes =
  print_header "streaming" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst = Mem_mst.of_assoc store data in
      let%lwt r1 =
        time_it (Printf.sprintf "to_blocks_stream consume %d" size) (fun () ->
            let stream = Mem_mst.to_blocks_stream mst in
            Lwt_seq.fold_left_s (fun count _ -> Lwt.return (count + 1)) 0 stream )
      in
      print_result r1 ;
      let%lwt r2 =
        time_it (Printf.sprintf "to_ordered_stream consume %d" size) (fun () ->
            let stream = Mem_mst.to_ordered_stream mst in
            Lwt_seq.fold_left_s (fun count _ -> Lwt.return (count + 1)) 0 stream )
      in
      print_result r2 ; Lwt.return_unit )
    sizes

let bench_proof_generation sizes =
  print_header "proof generation" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst = Mem_mst.of_assoc store data in
      let num_proofs = min 10 size in
      let test_keys =
        shuffle data |> List.filteri (fun i _ -> i < num_proofs) |> List.map fst
      in
      let%lwt r =
        time_it
          (Printf.sprintf "proof_for_key %d proofs, %d-record tree" num_proofs
             size ) (fun () ->
            Lwt_list.iter_s
              (fun k -> Mem_mst.proof_for_key mst mst.root k >|= fun _ -> ())
              test_keys )
      in
      print_result r ;
      Printf.printf "    (%.6f s per proof)\n%!"
        (r.total_s /. float_of_int num_proofs) ;
      Lwt.return_unit )
    sizes

let bench_equality sizes =
  print_header "equality check" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst1 = Mem_mst.of_assoc store data in
      let%lwt mst2 = Mem_mst.of_assoc store (shuffle data) in
      let%lwt r =
        time_it (Printf.sprintf "equal (identical trees) %d records" size)
          (fun () -> Mem_mst.equal mst1 mst2 )
      in
      print_result r ; Lwt.return_unit )
    sizes

let bench_mixed_ops () =
  print_header "mixed operations" ;
  let configs = [(10000, 5000); (20000, 10000); (50000, 20000)] in
  Lwt_list.iter_s
    (fun (initial_size, num_ops) ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt initial_data = generate_bulk_data store initial_size in
      let%lwt mst = Mem_mst.of_assoc store initial_data in
      let%lwt extra_data = generate_bulk_data store num_ops in
      let existing = ref (shuffle initial_data) in
      let pending_adds = ref (shuffle extra_data) in
      let%lwt r =
        time_it
          (Printf.sprintf "mixed %d ops on %d-record tree" num_ops initial_size)
          (fun () ->
            let rec loop mst i =
              if i >= num_ops then Lwt.return mst
              else
                let op_type = Random.int 100 in
                if op_type < 70 then
                  (* add new record *)
                  match !pending_adds with
                  | (k, v) :: rest ->
                      pending_adds := rest ;
                      existing := (k, v) :: !existing ;
                      let%lwt mst' = Mem_mst.add mst k v in
                      loop mst' (i + 1)
                  | [] ->
                      loop mst (i + 1)
                else if op_type < 90 then
                  (* update existing record *)
                  match !existing with
                  | (k, _) :: _ ->
                      let%lwt new_cid = put_random_block store in
                      let%lwt mst' = Mem_mst.add mst k new_cid in
                      loop mst' (i + 1)
                  | [] ->
                      loop mst (i + 1)
                else
                  (* delete existing record *)
                  match !existing with
                  | (k, _) :: rest ->
                      existing := rest ;
                      let%lwt mst' = Mem_mst.delete mst k in
                      loop mst' (i + 1)
                  | [] ->
                      loop mst (i + 1)
            in
            loop mst 0 )
      in
      print_result r ;
      Printf.printf "    (%.6f s per op avg)\n%!"
        (r.total_s /. float_of_int num_ops) ;
      Lwt.return_unit )
    configs

let bench_batch_add () =
  print_header "batch add" ;
  let tree_size = 1000 in
  let batch_sizes = [100; 500; 1000; 2000] in
  Lwt_list.iter_s
    (fun batch_size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt initial_data = generate_bulk_data store tree_size in
      let%lwt mst = Mem_mst.of_assoc store initial_data in
      let%lwt batch_data = generate_bulk_data store batch_size in
      let%lwt r1 =
        time_it
          (Printf.sprintf "batch add (incremental) %d to %d tree" batch_size
             tree_size ) (fun () ->
            Lwt_list.fold_left_s
              (fun t (k, v) -> Mem_mst.add t k v)
              mst batch_data )
      in
      print_result r1 ;
      let%lwt r2 =
        time_it
          (Printf.sprintf "batch add (rebuild) %d to %d tree" batch_size
             tree_size ) (fun () ->
            Mem_mst.of_assoc store (initial_data @ batch_data) )
      in
      print_result r2 ;
      let speedup = r2.total_s /. r1.total_s in
      Printf.printf "  -> incremental is %.2fx %s for batch of %d\n%!"
        (abs_float speedup)
        (if speedup > 1.0 then "faster" else "slower")
        batch_size ;
      Lwt.return_unit )
    batch_sizes

(* testing proof generation with different key distributions *)
let bench_key_lookup_patterns sizes =
  print_header "key lookup patterns" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst = Mem_mst.of_assoc store data in
      let sorted_keys = List.sort compare (List.map fst data) in
      let num_lookups = min 20 size in
      (* first n keys *)
      let early_keys = List.filteri (fun i _ -> i < num_lookups) sorted_keys in
      let%lwt r1 =
        time_it
          (Printf.sprintf "proof early keys (%d from %d tree)" num_lookups size)
          (fun () ->
            Lwt_list.iter_s
              (fun k -> Mem_mst.proof_for_key mst mst.root k >|= fun _ -> ())
              early_keys )
      in
      print_result r1 ;
      (* last n keys *)
      let late_keys =
        List.filteri
          (fun i _ -> i >= List.length sorted_keys - num_lookups)
          sorted_keys
      in
      let%lwt r2 =
        time_it
          (Printf.sprintf "proof late keys (%d from %d tree)" num_lookups size)
          (fun () ->
            Lwt_list.iter_s
              (fun k -> Mem_mst.proof_for_key mst mst.root k >|= fun _ -> ())
              late_keys )
      in
      print_result r2 ;
      (* random keys *)
      let random_keys =
        shuffle sorted_keys |> List.filteri (fun i _ -> i < num_lookups)
      in
      let%lwt r3 =
        time_it
          (Printf.sprintf "proof random keys (%d from %d tree)" num_lookups size)
          (fun () ->
            Lwt_list.iter_s
              (fun k -> Mem_mst.proof_for_key mst mst.root k >|= fun _ -> ())
              random_keys )
      in
      print_result r3 ; Lwt.return_unit )
    sizes

let bench_serialization sizes =
  print_header "serialization" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst = Mem_mst.of_assoc store data in
      (* Retrieve the root node to serialize *)
      let%lwt root_node =
        match%lwt Mem_mst.retrieve_node mst mst.root with
        | Some n ->
            Lwt.return n
        | None ->
            failwith "root not found"
      in
      let iterations = 100 in
      let%lwt r =
        time_it_n (Printf.sprintf "serialize root node (%d-record tree)" size)
          iterations (fun () ->
            Mem_mst.serialize mst root_node >|= fun _ -> () )
      in
      print_result r ; Lwt.return_unit )
    sizes

let bench_layer_ops sizes =
  print_header "layer/height operations" ;
  Lwt_list.iter_s
    (fun size ->
      let store = Storage.Memory_blockstore.create () in
      let%lwt data = generate_bulk_data store size in
      let%lwt mst = Mem_mst.of_assoc store data in
      let iterations = 1000 in
      let%lwt r =
        time_it_n (Printf.sprintf "layer query (%d-record tree)" size)
          iterations (fun () -> Mem_mst.layer mst >|= fun _ -> () )
      in
      print_result r ; Lwt.return_unit )
    sizes

let run_all_benchmarks () =
  Printf.printf "mst benchmarks\n" ;
  Printf.printf "==============\n" ;
  let small = [500; 1000; 2500] in
  let medium = [1000; 2500; 5000] in
  let large = [5000; 10000; 20000] in
  let delete_configs = [(500, 1000); (1000, 2500); (2000, 5000)] in
  let%lwt () = bench_of_assoc large in
  let%lwt () = bench_incremental_add small in
  let%lwt () = bench_incremental_delete delete_configs in
  let%lwt () = bench_single_add_scaling medium in
  let%lwt () = bench_single_delete_scaling medium in
  let%lwt () = bench_traversal medium in
  let%lwt () = bench_streaming medium in
  let%lwt () = bench_proof_generation medium in
  let%lwt () = bench_equality small in
  let%lwt () = bench_mixed_ops () in
  let%lwt () = bench_batch_add () in
  let%lwt () = bench_key_lookup_patterns medium in
  let%lwt () = bench_serialization small in
  let%lwt () = bench_layer_ops medium in
  Lwt.return_unit

let () =
  Random.self_init () ;
  Lwt_main.run (run_all_benchmarks ())
