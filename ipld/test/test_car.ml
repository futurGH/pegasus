open Lwt.Syntax
open Lwt.Infix

let run f = Lwt_main.run (f ())

let bytes_chunks data =
  Bytes.to_seq data
  |> Seq.map (fun byte -> Bytes.make 1 byte)
  |> List.of_seq |> Lwt_seq.of_list

let test_chunked_roundtrip () =
  run
  @@ fun () ->
  let block = Bytes.of_string "streamed CAR block" in
  let root = Cid.create Dcbor block in
  let* car = Car.blocks_to_car root (Lwt_seq.of_list [(root, block)]) in
  let* roots, blocks = Car.read_car_stream (bytes_chunks car) in
  let* blocks = Lwt_seq.to_list blocks in
  Alcotest.(check int) "one root" 1 (List.length roots) ;
  Alcotest.(check bool) "root preserved" true (Cid.equal root (List.hd roots)) ;
  Alcotest.(check int) "one block" 1 (List.length blocks) ;
  let actual_cid, actual_block = List.hd blocks in
  Alcotest.(check bool) "block CID preserved" true (Cid.equal root actual_cid) ;
  Alcotest.(check bytes) "block bytes preserved" block actual_block ;
  Lwt.return_unit

let test_truncated_stream () =
  let block = Bytes.of_string "truncated" in
  let root = Cid.create Dcbor block in
  let car =
    run (fun () -> Car.blocks_to_car root (Lwt_seq.of_list [(root, block)]))
  in
  let truncated = Bytes.sub car 0 (Bytes.length car - 1) in
  Alcotest.check_raises "truncated CAR is rejected"
    (Failure "unexpected end of car stream") (fun () ->
      run
      @@ fun () ->
      let* _, blocks = Car.read_car_stream (bytes_chunks truncated) in
      let* _ = Lwt_seq.to_list blocks in
      Lwt.return_unit )

let test_oversized_section () =
  let prefix = Car.Varint.encode ((64 * 1024 * 1024) + 1) in
  Alcotest.check_raises "oversized section is rejected"
    (Failure "CAR section exceeds 67108864 bytes") (fun () ->
      run (fun () -> Car.read_car_stream (Lwt_seq.of_list [prefix]) >|= ignore) )

let test_invalid_header_version () =
  let header =
    Dag_cbor.encode
      (`Map
         (Dag_cbor.String_map.of_list
            [("version", `Integer 2L); ("roots", `Array [||])] ) )
  in
  let data = Bytes.cat (Car.Varint.encode (Bytes.length header)) header in
  Alcotest.check_raises "invalid CAR version is rejected"
    (Failure "CAR header must declare version 1") (fun () ->
      run (fun () -> Car.read_car_stream (Lwt_seq.of_list [data]) >|= ignore) )

let test_source_failure_propagates () =
  let stream () = Lwt.fail_with "source failed" in
  Alcotest.check_raises "stream source failure propagates"
    (Failure "source failed") (fun () ->
      run (fun () -> Car.read_car_stream stream >|= ignore) )

let () =
  Alcotest.run "car"
    [ ( "streaming parser"
      , [ ("chunked roundtrip", `Quick, test_chunked_roundtrip)
        ; ("truncated", `Quick, test_truncated_stream)
        ; ("oversized section", `Quick, test_oversized_section)
        ; ("invalid header", `Quick, test_invalid_header_version)
        ; ("source failure", `Quick, test_source_failure_propagates) ] ) ]
