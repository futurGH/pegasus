let bytes_from_list ints =
  let buf = Buffer.create (List.length ints) in
  List.iter (Buffer.add_uint8 buf) ints ;
  Buffer.to_bytes buf

let test_encode () =
  let cid =
    match
      Cid.of_string
        "bafyreihffx5a2e7k5uwrmmgofbvzujc5cmw5h4espouwuxt3liqoflx3ee"
    with
    | Ok cid ->
        cid
    | Error msg ->
        failwith msg
  in
  Alcotest.(check int) "version" 1 cid.version ;
  Alcotest.(check bool) "codec" true (cid.codec = Cid.Dcbor) ;
  Alcotest.(check bytes)
    "digest"
    (bytes_from_list
       [ 229
       ; 45
       ; 250
       ; 13
       ; 19
       ; 234
       ; 237
       ; 45
       ; 22
       ; 48
       ; 206
       ; 40
       ; 107
       ; 154
       ; 36
       ; 93
       ; 19
       ; 45
       ; 211
       ; 240
       ; 146
       ; 123
       ; 169
       ; 106
       ; 94
       ; 123
       ; 90
       ; 32
       ; 226
       ; 174
       ; 251
       ; 33 ] )
    cid.digest.contents ;
  Alcotest.(check bytes)
    "bytes"
    (bytes_from_list
       [ 1
       ; 113
       ; 18
       ; 32
       ; 229
       ; 45
       ; 250
       ; 13
       ; 19
       ; 234
       ; 237
       ; 45
       ; 22
       ; 48
       ; 206
       ; 40
       ; 107
       ; 154
       ; 36
       ; 93
       ; 19
       ; 45
       ; 211
       ; 240
       ; 146
       ; 123
       ; 169
       ; 106
       ; 94
       ; 123
       ; 90
       ; 32
       ; 226
       ; 174
       ; 251
       ; 33 ] )
    cid.bytes

let test_encode_empty_string () =
  let cid =
    match Cid.of_string "bafyreaa" with
    | Ok cid ->
        cid
    | Error msg ->
        failwith msg
  in
  Alcotest.(check int) "version" 1 cid.version ;
  Alcotest.(check bool) "codec" true (cid.codec = Cid.Dcbor) ;
  Alcotest.(check bytes) "digest" (bytes_from_list []) cid.digest.contents ;
  Alcotest.(check bytes) "bytes" (bytes_from_list [1; 113; 18; 0]) cid.bytes

let test_decode () =
  let buf =
    bytes_from_list
      [ 1
      ; 113
      ; 18
      ; 32
      ; 114
      ; 82
      ; 82
      ; 62
      ; 101
      ; 145
      ; 251
      ; 143
      ; 229
      ; 83
      ; 214
      ; 127
      ; 245
      ; 90
      ; 134
      ; 248
      ; 64
      ; 68
      ; 180
      ; 106
      ; 62
      ; 65
      ; 118
      ; 225
      ; 12
      ; 88
      ; 250
      ; 82
      ; 154
      ; 74
      ; 171
      ; 213 ]
  in
  let cid = Cid.decode buf in
  Alcotest.(check int) "version" 1 cid.version ;
  Alcotest.(check bool) "codec" true (cid.codec = Cid.Dcbor) ;
  Alcotest.(check bytes)
    "digest"
    (bytes_from_list
       [ 114
       ; 82
       ; 82
       ; 62
       ; 101
       ; 145
       ; 251
       ; 143
       ; 229
       ; 83
       ; 214
       ; 127
       ; 245
       ; 90
       ; 134
       ; 248
       ; 64
       ; 68
       ; 180
       ; 106
       ; 62
       ; 65
       ; 118
       ; 225
       ; 12
       ; 88
       ; 250
       ; 82
       ; 154
       ; 74
       ; 171
       ; 213 ] )
    cid.digest.contents ;
  Alcotest.(check bytes)
    "bytes"
    (bytes_from_list
       [ 1
       ; 113
       ; 18
       ; 32
       ; 114
       ; 82
       ; 82
       ; 62
       ; 101
       ; 145
       ; 251
       ; 143
       ; 229
       ; 83
       ; 214
       ; 127
       ; 245
       ; 90
       ; 134
       ; 248
       ; 64
       ; 68
       ; 180
       ; 106
       ; 62
       ; 65
       ; 118
       ; 225
       ; 12
       ; 88
       ; 250
       ; 82
       ; 154
       ; 74
       ; 171
       ; 213 ] )
    cid.bytes

let test_decode_empty_string () =
  let buf = bytes_from_list [1; 113; 18; 0] in
  let cid = Cid.decode buf in
  Alcotest.(check int) "version" 1 cid.version ;
  Alcotest.(check bool) "codec" true (cid.codec = Cid.Dcbor) ;
  Alcotest.(check bytes) "digest" (bytes_from_list []) cid.digest.contents ;
  Alcotest.(check bytes) "bytes" (bytes_from_list [1; 113; 18; 0]) cid.bytes

let test_create () =
  let cid = Cid.(create Dcbor (Bytes.of_string "abc")) in
  let str = Result.get_ok (Cid.to_string cid) in
  Alcotest.(check string)
    "digest" "bafyreif2pall7dybz7vecqka3zo24irdwabwdi4wc55jznaq75q7eaavvu" str

let test_create_empty_string () =
  let cid = Cid.(create_empty Dcbor) in
  Alcotest.(check int) "version" 1 cid.version ;
  Alcotest.(check bool) "codec" true (cid.codec = Cid.Dcbor) ;
  Alcotest.(check bytes) "digest" (bytes_from_list []) cid.digest.contents ;
  Alcotest.(check bytes) "bytes" (bytes_from_list [1; 113; 18; 0]) cid.bytes

let () =
  Alcotest.run "cid"
    [ ( "cid encoding"
      , [ ("encode", `Quick, test_encode)
        ; ("encode_empty_string", `Quick, test_encode_empty_string) ] )
    ; ( "cid decoding"
      , [ ("decode", `Quick, test_decode)
        ; ("decode_empty_string", `Quick, test_decode_empty_string) ] )
    ; ( "cid creation"
      , [ ("create", `Quick, test_create)
        ; ("create_empty_string", `Quick, test_create_empty_string) ] ) ]
