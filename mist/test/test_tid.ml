module Tid = Mist.Tid

let test_create () =
  Alcotest.(check string)
    "tid" "3kztsgrxiyxje"
    (Tid.of_timestamp_ms 1723819911723L ~clockid:490)

let test_invalid_create () =
  Alcotest.check_raises "non-negative"
    (Invalid_argument "timestamp must be within range [0, 2^53)") (fun () ->
      ignore (Tid.of_timestamp_us (-1L) ~clockid:490) ) ;
  Alcotest.check_raises "too large"
    (Invalid_argument "timestamp must be within range [0, 2^53)") (fun () ->
      ignore (Tid.of_timestamp_us (Int64.shift_left 1L 53) ~clockid:490) )

let test_clockid () =
  Alcotest.check_raises "clockid too large"
    (Invalid_argument "clockid must be within range [0, 1023]") (fun () ->
      ignore (Tid.of_timestamp_ms 1723819911723L ~clockid:1024) ) ;
  Alcotest.check_raises "clockid too small"
    (Invalid_argument "clockid must be within range [0, 1023]") (fun () ->
      ignore (Tid.of_timestamp_ms 1723819911723L ~clockid:(-1)) ) ;
  (* shouldn't throw *)
  ignore (Tid.of_timestamp_ms 1723819911723L ~clockid:0) ;
  (* shouldn't throw *)
  ignore (Tid.of_timestamp_ms 1723819911723L ~clockid:1023)

let test_parse () =
  let timestamp, clockid = Tid.to_timestamp_ms "3kztrqxakokct" in
  Alcotest.(check int64) "timestamp" timestamp 1723819179066L ;
  Alcotest.(check int) "clockid" clockid 281

let test_validate () =
  let valid_tids = ["3jzfcijpj2z2a"; "7777777777777"; "3zzzzzzzzzzzz"] in
  let invalid_tids =
    [ "3jzfcijpj2z21"
    ; "0000000000000"
    ; "3jzfcijpj2z2aa"
    ; "3jzfcijpj2z2"
    ; "3jzf-cij-pj2z-2a"
    ; "zzzzzzzzzzzzz"
    ; "kjzfcijpj2z2a"
    ; "kjzfcijpj2z2a" ]
  in
  List.iter
    (fun tid -> Alcotest.(check bool) ("valid " ^ tid) true (Tid.is_valid tid))
    valid_tids ;
  List.iter
    (fun tid ->
      Alcotest.(check bool) ("invalid " ^ tid) false (Tid.is_valid tid) )
    invalid_tids

let () =
  Alcotest.run "tid"
    [ ( "create"
      , [ ("create", `Quick, test_create)
        ; ("invalid timestamp", `Quick, test_invalid_create)
        ; ("invalid clockid", `Quick, test_clockid) ] )
    ; ("parse", [("parse", `Quick, test_parse)])
    ; ("validate", [("validate", `Quick, test_validate)]) ]
