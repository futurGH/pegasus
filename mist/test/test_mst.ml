open Mist
module MemMst = Mst.Make (Storage.Memory_blockstore)
module MemRepo = Repository.Make (Storage.Memory_blockstore)

let test_roundtrip () =
  let open Lwt.Infix in
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
      MemRepo.read_commit store root
      >|= function Ok commit -> commit | Error msg -> failwith msg
    in
    let mst = MemMst.create store commit.data in
    Lwt.return (commit, mst)
  in
  let%lwt ic = Lwt_io.open_file ~mode:Lwt_io.input "sample.car" in
  let%lwt car = Lwt_io.read ic >|= Bytes.of_string in
  let%lwt () = Lwt_io.close ic in
  let%lwt commit, mst = mst_of_car_bytes car in
  let mst_stream = MemMst.to_blocks_stream mst in
  let commit_bytes =
    Dag_cbor.encode_yojson (Repository.signed_commit_to_yojson commit)
  in
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
    [ ( "mst roundtrip"
      , [ test_case "car→mst→car→mst roundtrip" `Quick (fun () ->
              Lwt_main.run (test_roundtrip ()) ) ] ) ]
