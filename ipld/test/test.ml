let () =
  Alcotest.run "cid"
    [ ( "cid encoding"
      , [ ("encode", `Quick, Test_cid.test_encode)
        ; ("encode_empty_string", `Quick, Test_cid.test_encode_empty_string) ]
      )
    ; ( "cid decoding"
      , [ ("decode", `Quick, Test_cid.test_decode)
        ; ("decode_empty_string", `Quick, Test_cid.test_decode_empty_string) ]
      )
    ; ( "cid creation"
      , [ ("create", `Quick, Test_cid.test_create)
        ; ("create_empty_string", `Quick, Test_cid.test_create_empty_string) ]
      ) ]

let () =
  Alcotest.run "dag-cbor"
    [ ( "dag-cbor encoding"
      , [ ("encode_primitives", `Quick, Test_dag_cbor.test_encode_primitives)
        ; ("round_trip", `Quick, Test_dag_cbor.test_round_trip)
        ; ("atproto_records", `Quick, Test_dag_cbor.test_atproto_post_records)
        ; ("invalid_numbers", `Quick, Test_dag_cbor.test_invalid_numbers)
        ; ( "invalid_link_bytes"
          , `Quick
          , Test_dag_cbor.test_invalid_link_and_bytes )
        ; ("decode_multiple", `Quick, Test_dag_cbor.test_decode_multiple_objects)
        ] ) ]
