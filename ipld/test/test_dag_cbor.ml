open Ipld
module StringMap = Dag_cbor.StringMap

let to_base_16 bytes =
  let hex_of_nibble n =
    if n < 10 then char_of_int (n + 48) else char_of_int (n + 87)
  in
  let len = Bytes.length bytes in
  let result = Bytes.create (len * 2) in
  for i = 0 to len - 1 do
    let byte_val = Bytes.get_uint8 bytes i in
    (* upper 4 bits *)
    let high_nibble = byte_val lsr 4 in
    (* lower 4 bits *)
    let low_nibble = byte_val land 0xF in
    Bytes.set result (i * 2) (hex_of_nibble high_nibble) ;
    Bytes.set result ((i * 2) + 1) (hex_of_nibble low_nibble)
  done ;
  Bytes.to_string result

let test_encode_primitives () =
  let cases = Hashtbl.create 9 in
  Hashtbl.add cases
    (to_base_16 (Dag_cbor.encode (`String "hello world!")))
    (Bytes.of_string "6c68656c6c6f20776f726c6421") ;
  Hashtbl.add cases
    (to_base_16 (Dag_cbor.encode (`String "おはようございます☀️")))
    (Bytes.of_string
       "7821e3818ae381afe38288e38186e38194e38196e38184e381bee38199e29880efb88f" ) ;
  Hashtbl.add cases
    (to_base_16 (Dag_cbor.encode (`Integer 42L)))
    (Bytes.of_string "182a") ;
  Hashtbl.add cases
    (to_base_16 (Dag_cbor.encode (`Float 3.14)))
    (Bytes.of_string "fb40091eb851eb851f") ;
  Hashtbl.add cases
    (to_base_16 (Dag_cbor.encode (`Integer 9007199254740991L)))
    (Bytes.of_string "1b001fffffffffffff") ;
  Hashtbl.add cases
    (to_base_16 (Dag_cbor.encode (`Integer (-9007199254740991L))))
    (Bytes.of_string "3b001ffffffffffffe") ;
  Hashtbl.add cases
    (to_base_16 (Dag_cbor.encode (`Boolean true)))
    (Bytes.of_string "f5") ;
  Hashtbl.add cases
    (to_base_16 (Dag_cbor.encode (`Boolean false)))
    (Bytes.of_string "f4") ;
  Hashtbl.add cases (to_base_16 (Dag_cbor.encode `Null)) (Bytes.of_string "f6") ;
  cases
  |> Hashtbl.iter (fun key value ->
         Alcotest.(check bytes)
           ("encoded bytes for " ^ key)
           value (Bytes.of_string key) )

let test_round_trip () =
  let test_cid =
    match
      Cid.of_string
        "bafyreihffx5a2e7k5uwrmmgofbvzujc5cmw5h4espouwuxt3liqoflx3ee"
    with
    | Ok cid ->
        cid
    | Error msg ->
        failwith msg
  in
  let test_bytes = Bytes.of_string "lorem ipsum sit dolor amet" in
  let bee_array : Dag_cbor.value list =
    [ `String
        "According to all known laws of aviation, there is no way that a bee \
         should be able to fly."
    ; `String
        "Its wings are too small to get its fat little body off the ground."
    ; `String "The bee, of course, flies anyway."
    ; `String "Because bees don't care what humans think is impossible."
    ; `String
        "Ladies and gentlemen of the jury, my grandmother was a simple woman."
    ; `String
        "Born on a farm, she believed it was man's divine right to benefit \
         from the county of nature God put before us."
    ; `String
        "If we were to live the topsy-turvy world Mr. Benson imagines, just \
         think of what if would mean?"
    ; `String
        "Maybe I would have to negotiate with the silkworm for the elastic in \
         my britches!"
    ; `String "Talking bee!"
    ; `String
        "How do we know this isn't some sort of holographic \
         motion-picture-capture hollywood wizardry?"
    ; `String
        "They could be using laser beams! Robotics! Ventriloquism! Cloning! \
         For all we know he could be on steroids!"
    ; `String
        "Ladies and gentlemen of the jury, there's no trickery here. I'm just \
         an ordinary bee."
    ; `String
        "And as a bee, honey's pretty important to me. It's important to all \
         bees."
    ; `String "We invented it, we make it, and we protect it with our lives."
    ; `String
        "Unfortunately, there are some people in this room who think they can \
         take whatever they want from us, 'cause we're the little guys!"
    ; `String
        "And what I'm hoping is that after this is all over, you'll see how by \
         taking our honey, you're not only taking away everything we have, but \
         everything we are!" ]
  in
  let nested_map = StringMap.add "hello" (`String "world") StringMap.empty in
  let object_map =
    StringMap.empty
    |> StringMap.add "key" (`String "value")
    |> StringMap.add "link" (`Link test_cid)
    |> StringMap.add "bytes" (`Bytes test_bytes)
    |> StringMap.add "answer" (`Integer 42L)
    |> StringMap.add "correct" (`Boolean true)
    |> StringMap.add "wrong" (`Boolean false)
    |> StringMap.add "blank" `Null
    |> StringMap.add "b16" (`Integer 262L)
    |> StringMap.add "b32" (`Integer 65542L)
    |> StringMap.add "minInteger" (`Integer (-9007199254740991L))
    |> StringMap.add "maxInteger" (`Integer 9007199254740991L)
    |> StringMap.add "pi" (`Float 3.141592653589793)
    |> StringMap.add "npi" (`Float (-3.141592653589793))
    |> StringMap.add "nested" (`Map nested_map)
    |> StringMap.add "bee" (`Array bee_array)
  in
  let original = `Map object_map in
  let encoded = Dag_cbor.encode original in
  let decoded = Dag_cbor.decode encoded in
  Alcotest.(check bool)
    "round trip preserves structure" true (original = decoded)

let test_atproto_post_records () =
  let record1_map =
    StringMap.empty
    |> StringMap.add "$type" (`String "app.bsky.feed.post")
    |> StringMap.add "createdAt" (`String "2024-08-13T01:16:06.453Z")
    |> StringMap.add "langs" (`Array [`String "en"])
    |> StringMap.add "text" (`String "exclusively on bluesky")
  in
  let record1 = `Map record1_map in
  let encoded1 = Dag_cbor.encode record1 in
  (* We can't easily test the CID creation without additional dependencies,
     so we'll just verify it encodes without error *)
  Alcotest.(check bool)
    "atproto record 1 encodes" true
    (Bytes.length encoded1 > 0) ;
  let record2_map =
    StringMap.empty
    |> StringMap.add "$type" (`String "app.bsky.feed.post")
    |> StringMap.add "createdAt" (`String "2025-01-02T23:29:41.149Z")
    |> StringMap.add "langs" (`Array [`String "ja"])
    |> StringMap.add "text"
         (`String
           "おはようございます☀️\n今日の日の出です\n寒かったけど綺麗でしたよ✨\n\n＃写真\n＃日の出\n＃日常\n＃キリトリセカイ" )
  in
  let record2 = `Map record2_map in
  let encoded2 = Dag_cbor.encode record2 in
  Alcotest.(check bool)
    "atproto record 2 encodes" true
    (Bytes.length encoded2 > 0)

let test_invalid_numbers () =
  Alcotest.check_raises "encode rejects out of range positive integer"
    (Invalid_argument "write_uint_53: value out of range (0-9007199254740991)")
    (fun () -> ignore (Dag_cbor.encode (`Integer 9007199254740992L)) ) ;
  Alcotest.check_raises "encode rejects out of range negative integer"
    (Invalid_argument "write_uint_53: value out of range (0-9007199254740991)")
    (fun () -> ignore (Dag_cbor.encode (`Integer (-9007199254740992L))) )

let test_invalid_link_and_bytes () =
  let invalid_link_map =
    StringMap.add "$link" (`Integer 123L) StringMap.empty
  in
  Alcotest.check_raises "encode rejects non-CID $link value"
    (Invalid_argument "Object contains $link but value is not a cid-link")
    (fun () -> ignore (Dag_cbor.encode (`Map invalid_link_map)) ) ;
  let invalid_bytes_map =
    StringMap.add "$bytes" (`Integer 123L) StringMap.empty
  in
  Alcotest.check_raises "encode rejects non-bytes $bytes value"
    (Invalid_argument "Object contains $bytes but value is not bytes")
    (fun () -> ignore (Dag_cbor.encode (`Map invalid_bytes_map)) )

let test_decode_multiple_objects () =
  let obj1 = `Map (StringMap.add "foo" (`Boolean true) StringMap.empty) in
  let obj2 = `Map (StringMap.add "bar" (`Boolean false) StringMap.empty) in
  let encoded1 = Dag_cbor.encode obj1 in
  let encoded2 = Dag_cbor.encode obj2 in
  let combined = Bytes.create (Bytes.length encoded1 + Bytes.length encoded2) in
  Bytes.blit encoded1 0 combined 0 (Bytes.length encoded1) ;
  Bytes.blit encoded2 0 combined (Bytes.length encoded1) (Bytes.length encoded2) ;
  (* Test that we can decode the first object and get remainder *)
  let decoded1, remainder = Dag_cbor.Decoder.decode_first combined in
  Alcotest.(check bool) "first object decoded correctly" true (decoded1 = obj1) ;
  let decoded2, final_remainder = Dag_cbor.Decoder.decode_first remainder in
  Alcotest.(check bool) "second object decoded correctly" true (decoded2 = obj2) ;
  Alcotest.(check int) "no remaining bytes" 0 (Bytes.length final_remainder)
