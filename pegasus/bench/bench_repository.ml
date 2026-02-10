open Lwt.Infix
module Block_map = Mist.Storage.Block_map
module Lex = Mist.Lex
module Tid = Mist.Tid
module User_store = Pegasus.User_store
module Util = Pegasus.Util
module Migrations = Pegasus.Migrations
module Mst = Mist.Mst.Make (User_store)
module Mem_mst = Mist.Mst.Make (Mist.Storage.Memory_blockstore)

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
  if r.iterations = 1 then Printf.printf "  %-55s %10.4f s\n%!" r.name r.total_s
  else
    Printf.printf "  %-55s %10.4f s total, %10.6f s/iter (%d iters)\n%!" r.name
      r.total_s r.per_iter_s r.iterations

let print_header name = Printf.printf "\n=== %s ===\n%!" name

let temp_db_counter = ref 0

let create_temp_db () =
  incr temp_db_counter ;
  let path =
    Printf.sprintf "/tmp/pegasus_bench_%d_%d.db" (Unix.getpid ())
      !temp_db_counter
  in
  let uri = Uri.of_string ("sqlite3://" ^ path) in
  (path, uri)

let cleanup_temp_db path =
  try Unix.unlink path
  with Unix.Unix_error _ -> (
    () ;
    try Unix.unlink (path ^ "-wal")
    with Unix.Unix_error _ -> (
      () ;
      try Unix.unlink (path ^ "-shm") with Unix.Unix_error _ -> () ) )

let setup_test_db () : (User_store.t * string) Lwt.t =
  let path, uri = create_temp_db () in
  let%lwt pool = Util.Sqlite.connect ~create:true ~write:true uri in
  let%lwt () = Migrations.run_migrations User_store pool in
  let db : User_store.t = {did= "did:plc:bench"; db= pool} in
  Lwt.return (db, path)

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

let random_alnum len =
  let allowed = "abcdefghijklmnopqrstuvwxyz0123456789" in
  let b = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.set b i allowed.[Random.int (String.length allowed)]
  done ;
  Bytes.to_string b

let valid_rkey () = random_alnum 13

let make_path () = "app.bsky.feed.post/" ^ valid_rkey ()

let rec unique_paths n acc =
  if n <= 0 then acc
  else
    let p = make_path () in
    if List.mem p acc then unique_paths n acc
    else unique_paths (n - 1) (p :: acc)

let generate_blocks count = List.init count (fun _ -> random_block ())

let generate_record_data count =
  let paths = unique_paths count [] in
  List.map
    (fun path ->
      let record : Lex.repo_record =
        Lex.String_map.empty
        |> Lex.String_map.add "$type" (`String "app.bsky.feed.post")
        |> Lex.String_map.add "text" (`String (random_alnum 100))
        |> Lex.String_map.add "createdAt" (`String (Tid.now ()))
      in
      let cid, data = Lex.to_cbor_block (`LexMap record) in
      (path, cid, data, Tid.now ()) )
    paths

let shuffle lst =
  let arr = Array.of_list lst in
  for i = Array.length arr - 1 downto 1 do
    let j = Random.int (i + 1) in
    let tmp = arr.(i) in
    arr.(i) <- arr.(j) ;
    arr.(j) <- tmp
  done ;
  Array.to_list arr

let bench_single_block_ops () =
  print_header "single block ops" ;
  let%lwt db, path = setup_test_db () in
  let blocks = generate_blocks 1000 in
  let%lwt r1 =
    time_it_n "put_block (single)" 1000 (fun () ->
        let cid, data = List.nth blocks (Random.int 1000) in
        User_store.put_block db cid data >|= fun _ -> () )
  in
  print_result r1 ;
  let%lwt () =
    Lwt_list.iter_s
      (fun (cid, data) -> User_store.put_block db cid data >|= fun _ -> ())
      blocks
  in
  let%lwt r2 =
    time_it_n "get_bytes (single, existing)" 1000 (fun () ->
        let cid, _ = List.nth blocks (Random.int 1000) in
        User_store.get_bytes db cid >|= fun _ -> () )
  in
  print_result r2 ;
  let missing_cid, _ = random_block () in
  let%lwt r3 =
    time_it_n "get_bytes (single, missing)" 1000 (fun () ->
        User_store.get_bytes db missing_cid >|= fun _ -> () )
  in
  print_result r3 ; cleanup_temp_db path ; Lwt.return_unit

let bench_batch_block_ops () =
  print_header "batch block ops" ;
  let batch_sizes = [100; 500; 1000; 2000] in
  Lwt_list.iter_s
    (fun batch_size ->
      let%lwt db, path = setup_test_db () in
      let blocks = generate_blocks batch_size in
      let block_map =
        List.fold_left
          (fun acc (cid, data) -> Block_map.set cid data acc)
          Block_map.empty blocks
      in
      let%lwt r1 =
        time_it (Printf.sprintf "put_many (%d blocks)" batch_size) (fun () ->
            User_store.put_many db block_map >|= fun _ -> () )
      in
      print_result r1 ;
      let cids = List.map fst blocks in
      let%lwt r2 =
        time_it (Printf.sprintf "get_blocks (%d blocks)" batch_size) (fun () ->
            User_store.get_blocks db cids >|= fun _ -> () )
      in
      print_result r2 ; cleanup_temp_db path ; Lwt.return_unit )
    batch_sizes

let bench_bulk_insert_ops () =
  print_header "bulk insert" ;
  let sizes = [1000; 5000; 10000; 20000] in
  Lwt_list.iter_s
    (fun size ->
      let%lwt db, path = setup_test_db () in
      let blocks = generate_blocks size in
      let%lwt r1 =
        time_it (Printf.sprintf "Bulk.put_blocks (%d blocks)" size) (fun () ->
            Util.Sqlite.use_pool db.db (fun conn ->
                User_store.Bulk.put_blocks blocks conn )
            >|= fun _ -> () )
      in
      print_result r1 ;
      cleanup_temp_db path ;
      let%lwt db2, path2 = setup_test_db () in
      let records = generate_record_data size in
      let%lwt r2 =
        time_it (Printf.sprintf "Bulk.put_records (%d records)" size) (fun () ->
            Util.Sqlite.use_pool db2.db (fun conn ->
                User_store.Bulk.put_records records conn )
            >|= fun _ -> () )
      in
      print_result r2 ; cleanup_temp_db path2 ; Lwt.return_unit )
    sizes

let bench_db_mst_ops () =
  print_header "mst operations (with db)" ;
  let sizes = [1000; 5000; 10000] in
  Lwt_list.iter_s
    (fun size ->
      let%lwt db, path = setup_test_db () in
      let records = generate_record_data size in
      let kv_pairs = List.map (fun (path, cid, _, _) -> (path, cid)) records in
      let%lwt () =
        Util.Sqlite.use_pool db.db (fun conn ->
            User_store.Bulk.put_records records conn )
        >|= fun _ -> ()
      in
      let%lwt r1 =
        time_it (Printf.sprintf "Mst.of_assoc (db-backed, %d records)" size)
          (fun () -> Mst.of_assoc db kv_pairs >|= fun _ -> () )
      in
      print_result r1 ;
      let%lwt mst = Mst.of_assoc db kv_pairs in
      let%lwt r2 =
        time_it (Printf.sprintf "Mst.build_map (db-backed, %d records)" size)
          (fun () -> Mst.build_map mst >|= fun _ -> () )
      in
      print_result r2 ;
      let%lwt r3 =
        time_it (Printf.sprintf "Mst.leaf_count (db-backed, %d records)" size)
          (fun () -> Mst.leaf_count mst >|= fun _ -> () )
      in
      print_result r3 ; cleanup_temp_db path ; Lwt.return_unit )
    sizes

let bench_db_mst_incremental () =
  print_header "incremental mst add (with db)" ;
  let configs = [(5000, 50); (10000, 100)] in
  Lwt_list.iter_s
    (fun (initial_size, add_count) ->
      let%lwt db, path = setup_test_db () in
      let initial_records = generate_record_data initial_size in
      let initial_kv =
        List.map (fun (path, cid, _, _) -> (path, cid)) initial_records
      in
      let%lwt () =
        Util.Sqlite.use_pool db.db (fun conn ->
            User_store.Bulk.put_records initial_records conn )
        >|= fun _ -> ()
      in
      let%lwt mst = Mst.of_assoc db initial_kv in
      let add_records = generate_record_data add_count in
      let add_kv =
        List.map (fun (path, cid, _, _) -> (path, cid)) add_records
      in
      let%lwt () =
        Util.Sqlite.use_pool db.db (fun conn ->
            User_store.Bulk.put_records add_records conn )
        >|= fun _ -> ()
      in
      let%lwt r1 =
        time_it
          (Printf.sprintf "Mst.add incremental (%d to %d-record tree)" add_count
             initial_size ) (fun () ->
            Lwt_list.fold_left_s (fun t (k, v) -> Mst.add t k v) mst add_kv
            >|= fun _ -> () )
      in
      print_result r1 ;
      Printf.printf "    (%.6f s per add avg)\n%!"
        (r1.total_s /. float_of_int add_count) ;
      cleanup_temp_db path ;
      Lwt.return_unit )
    configs

let bench_car_export () =
  print_header "car export" ;
  let sizes = [1000; 5000; 10000; 20000] in
  Lwt_list.iter_s
    (fun size ->
      let store = Mist.Storage.Memory_blockstore.create () in
      let records = generate_record_data size in
      let%lwt () =
        Lwt_list.iter_s
          (fun (_, cid, data, _) ->
            Mist.Storage.Memory_blockstore.put_block store cid data
            >|= fun _ -> () )
          records
      in
      let kv_pairs = List.map (fun (path, cid, _, _) -> (path, cid)) records in
      let%lwt mst = Mem_mst.of_assoc store kv_pairs in
      let%lwt r1 =
        time_it (Printf.sprintf "to_blocks_stream + consume (%d records)" size)
          (fun () ->
            let stream = Mem_mst.to_blocks_stream mst in
            Lwt_seq.fold_left_s (fun count _ -> Lwt.return (count + 1)) 0 stream
            >|= fun _ -> () )
      in
      print_result r1 ;
      let commit_map =
        Dag_cbor.String_map.(empty |> add "root" (`Link mst.root))
      in
      let commit_block = Dag_cbor.encode (`Map commit_map) in
      let commit_cid = Cid.create Dcbor commit_block in
      let%lwt r2 =
        time_it (Printf.sprintf "Car.blocks_to_stream (%d records)" size)
          (fun () ->
            let blocks = Mem_mst.to_blocks_stream mst in
            let all_blocks = Lwt_seq.cons (commit_cid, commit_block) blocks in
            let car_stream = Car.blocks_to_stream commit_cid all_blocks in
            Lwt_seq.fold_left_s
              (fun acc chunk -> Lwt.return (acc + Bytes.length chunk))
              0 car_stream
            >|= fun _ -> () )
      in
      print_result r2 ; Lwt.return_unit )
    sizes

let bench_car_import () =
  print_header "car import" ;
  let sizes = [10000; 25000; 50000] in
  Lwt_list.iter_s
    (fun size ->
      let store = Mist.Storage.Memory_blockstore.create () in
      let records = generate_record_data size in
      let%lwt () =
        Lwt_list.iter_s
          (fun (_, cid, data, _) ->
            Mist.Storage.Memory_blockstore.put_block store cid data
            >|= fun _ -> () )
          records
      in
      let kv_pairs = List.map (fun (path, cid, _, _) -> (path, cid)) records in
      let%lwt mst = Mem_mst.of_assoc store kv_pairs in
      let commit_map =
        Dag_cbor.String_map.(empty |> add "root" (`Link mst.root))
      in
      let commit_block = Dag_cbor.encode (`Map commit_map) in
      let commit_cid = Cid.create Dcbor commit_block in
      let blocks = Mem_mst.to_blocks_stream mst in
      let all_blocks = Lwt_seq.cons (commit_cid, commit_block) blocks in
      let car_stream = Car.blocks_to_stream commit_cid all_blocks in
      let%lwt car_bytes = Car.collect_stream car_stream in
      Printf.printf "  (car size for %d records: %d bytes)\n%!" size
        (Bytes.length car_bytes) ;
      let%lwt r1 =
        time_it (Printf.sprintf "Car.read_car_stream parse (%d records)" size)
          (fun () ->
            let stream = Lwt_seq.return car_bytes in
            let%lwt roots, blocks_seq = Car.read_car_stream stream in
            let%lwt _ =
              Lwt_seq.fold_left_s
                (fun acc (_, _) -> Lwt.return (acc + 1))
                0 blocks_seq
            in
            Lwt.return roots )
      in
      print_result r1 ; Lwt.return_unit )
    sizes

let bench_rebuild_mst () =
  print_header "mst rebuild" ;
  let sizes = [1000; 2500; 5000] in
  Lwt_list.iter_s
    (fun size ->
      let%lwt db, path = setup_test_db () in
      let records = generate_record_data size in
      let%lwt () =
        Util.Sqlite.use_pool db.db (fun conn ->
            User_store.Bulk.put_records records conn )
        >|= fun _ -> ()
      in
      let%lwt r1 =
        time_it
          (Printf.sprintf "get_all_record_cids + of_assoc (%d records)" size)
          (fun () ->
            let%lwt record_cids = User_store.get_all_record_cids db in
            let%lwt _ = Mst.of_assoc db record_cids in
            Lwt.return_unit )
      in
      print_result r1 ; cleanup_temp_db path ; Lwt.return_unit )
    sizes

let bench_mixed_ops () =
  print_header "mixed operations" ;
  let%lwt db, path = setup_test_db () in
  let initial_size = 1000 in
  let num_ops = 500 in
  let initial_records = generate_record_data initial_size in
  let%lwt () =
    Util.Sqlite.use_pool db.db (fun conn ->
        User_store.Bulk.put_records initial_records conn )
    >|= fun _ -> ()
  in
  let initial_kv =
    List.map (fun (path, cid, _, _) -> (path, cid)) initial_records
  in
  let%lwt mst = Mst.of_assoc db initial_kv in
  let extra_records = generate_record_data num_ops in
  let%lwt () =
    Util.Sqlite.use_pool db.db (fun conn ->
        User_store.Bulk.put_records extra_records conn )
    >|= fun _ -> ()
  in
  let existing = ref (shuffle initial_records) in
  let pending_adds = ref (shuffle extra_records) in
  let%lwt r1 =
    time_it (Printf.sprintf "mixed ops (%d ops)" num_ops) (fun () ->
        let rec loop mst i =
          if i >= num_ops then Lwt.return mst
          else
            let op_type = Random.int 100 in
            if op_type < 60 then
              match !pending_adds with
              | (path, cid, _, _) :: rest ->
                  pending_adds := rest ;
                  existing := (path, cid, Bytes.empty, "") :: !existing ;
                  let%lwt mst' = Mst.add mst path cid in
                  loop mst' (i + 1)
              | [] ->
                  loop mst (i + 1)
            else if op_type < 90 then
              match !existing with
              | (path, _, _, _) :: _ ->
                  let%lwt _ = Mst.proof_for_key mst mst.root path in
                  loop mst (i + 1)
              | [] ->
                  loop mst (i + 1)
            else
              match !existing with
              | (path, _, _, _) :: rest ->
                  existing := rest ;
                  let%lwt mst' = Mst.delete mst path in
                  loop mst' (i + 1)
              | [] ->
                  loop mst (i + 1)
        in
        loop mst 0 >|= fun _ -> () )
  in
  print_result r1 ;
  Printf.printf "    (%.6f s per op avg)\n%!"
    (r1.total_s /. float_of_int num_ops) ;
  cleanup_temp_db path ;
  Lwt.return_unit

let bench_db_io_patterns () =
  print_header "database i/o" ;
  let%lwt db, path = setup_test_db () in
  let size = 20000 in
  let blocks = generate_blocks size in
  let%lwt () =
    Util.Sqlite.use_pool db.db (fun conn ->
        User_store.Bulk.put_blocks blocks conn )
    >|= fun _ -> ()
  in
  let cids = List.map fst blocks in
  let shuffled_cids = shuffle cids in
  let%lwt r1 =
    time_it (Printf.sprintf "sequential read (%d blocks)" size) (fun () ->
        Lwt_list.iter_s
          (fun cid -> User_store.get_bytes db cid >|= fun _ -> ())
          cids )
  in
  print_result r1 ;
  let%lwt r2 =
    time_it (Printf.sprintf "random read (%d blocks)" size) (fun () ->
        Lwt_list.iter_s
          (fun cid -> User_store.get_bytes db cid >|= fun _ -> ())
          shuffled_cids )
  in
  print_result r2 ;
  let batch_size = 50 in
  let batches =
    List.init (size / batch_size) (fun i ->
        List.filteri
          (fun j _ -> j >= i * batch_size && j < (i + 1) * batch_size)
          cids )
  in
  let%lwt r3 =
    time_it
      (Printf.sprintf "batched read (%d blocks, batch=%d)" size batch_size)
      (fun () ->
        Lwt_list.iter_s
          (fun batch -> User_store.get_blocks db batch >|= fun _ -> ())
          batches )
  in
  print_result r3 ; cleanup_temp_db path ; Lwt.return_unit

let run_all_benchmarks () =
  Printf.printf "repository benchmarks\n" ;
  Printf.printf "=====================\n" ;
  let%lwt () = bench_single_block_ops () in
  let%lwt () = bench_batch_block_ops () in
  let%lwt () = bench_bulk_insert_ops () in
  let%lwt () = bench_db_mst_ops () in
  let%lwt () = bench_db_mst_incremental () in
  let%lwt () = bench_car_export () in
  let%lwt () = bench_car_import () in
  let%lwt () = bench_rebuild_mst () in
  let%lwt () = bench_mixed_ops () in
  let%lwt () = bench_db_io_patterns () in
  Lwt.return_unit

let () =
  Random.self_init () ;
  Lwt_main.run (run_all_benchmarks ())
