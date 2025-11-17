module String_map = Dag_cbor.String_map

let rec stringify_map m =
  String_map.bindings m
  |> List.map (fun (k, v) ->
      Format.sprintf "\"%s\": %s" k (stringify_ipld_value v) )
  |> String.concat ", " |> Format.sprintf "{%s}"

and stringify_ipld_value (value : Dag_cbor.value) =
  match value with
  | `Boolean b ->
      Format.sprintf "%b" b
  | `Integer i ->
      Format.sprintf "%Ld" i
  | `Float f ->
      Format.sprintf "%f" f
  | `String s ->
      Format.sprintf "\"%s\""
        (Str.global_replace (Str.regexp_string {|"|}) {|\"|} s)
  | `Bytes b ->
      Format.sprintf "%s" (Bytes.to_string b)
  | `Map m ->
      Format.sprintf "%s" (stringify_map m)
  | `Array a ->
      Format.sprintf "[%s]"
        ( Array.to_list a
        |> List.map (fun v -> Format.sprintf "%s" (stringify_ipld_value v))
        |> String.concat ", " )
  | `Link cid ->
      Format.sprintf "%s"
        (stringify_map
           (String_map.singleton "$link" (`String (Cid.to_string cid))) )
  | `Null ->
      Format.sprintf "null"

let pprint_ipld_value ppf value = Fmt.pf ppf "%s" (stringify_ipld_value value)

let rec ipld_value_eq a b =
  match (a, b) with
  | `Boolean a, `Boolean b ->
      a = b
  | `Integer a, `Integer b ->
      a = b
  | `Float a, `Float b ->
      a = b
  | `String a, `String b ->
      a = b
  | `Bytes a, `Bytes b ->
      a = b
  | `Map a, `Map b ->
      String_map.equal ipld_value_eq a b
  | `Array a, `Array b ->
      Array.for_all2 ipld_value_eq a b
  | `Link a, `Link b ->
      Cid.to_string a = Cid.to_string b
  | `Null, `Null ->
      true
  | _ ->
      false

let ipld_testable = Alcotest.testable pprint_ipld_value ipld_value_eq

let yojson_testable = Alcotest.testable Yojson.Safe.pp Yojson.Safe.equal

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
  let bee_array : Dag_cbor.value array =
    [| `String
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
         "And what I'm hoping is that after this is all over, you'll see how \
          by taking our honey, you're not only taking away everything we have, \
          but everything we are!" |]
  in
  let nested_map = String_map.add "hello" (`String "world") String_map.empty in
  let object_map =
    String_map.empty
    |> String_map.add "key" (`String "value")
    |> String_map.add "link" (`Link test_cid)
    |> String_map.add "bytes" (`Bytes test_bytes)
    |> String_map.add "answer" (`Integer 42L)
    |> String_map.add "correct" (`Boolean true)
    |> String_map.add "wrong" (`Boolean false)
    |> String_map.add "blank" `Null
    |> String_map.add "b16" (`Integer 262L)
    |> String_map.add "b32" (`Integer 65542L)
    |> String_map.add "minInteger" (`Integer (-9007199254740991L))
    |> String_map.add "maxInteger" (`Integer 9007199254740991L)
    |> String_map.add "pi" (`Float 3.141592653589793)
    |> String_map.add "npi" (`Float (-3.141592653589793))
    |> String_map.add "nested" (`Map nested_map)
    |> String_map.add "bee" (`Array bee_array)
  in
  let original = `Map object_map in
  let encoded = Dag_cbor.encode original in
  let decoded = Dag_cbor.decode encoded in
  Alcotest.(check ipld_testable)
    "round trip preserves structure" original decoded

let test_atproto_post_records () =
  let record1_embed_images_0_aspect_ratio : Dag_cbor.value String_map.t =
    String_map.(
      empty |> add "height" (`Integer 885L) |> add "width" (`Integer 665L) )
  in
  let record1_embed_images_0_image : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "$type" (`String "blob")
      |> add "ref"
           (`Link
              (Result.get_ok
                 (Cid.of_string
                    "bafkreic6hvmy3ymbo25wxsvylu77r57uwhtnviu7vmhfsns3ab4xfal5ou" ) )
           )
      |> add "mimeType" (`String "image/jpeg")
      |> add "size" (`Integer 645553L) )
  in
  let record1_embed_images_0 : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "alt"
           (`String
              "a photoshopped picture of kit with a microphone. kit is saying \
               \"meow\"" )
      |> add "aspectRatio" (`Map record1_embed_images_0_aspect_ratio)
      |> add "image" (`Map record1_embed_images_0_image) )
  in
  let record1_embed : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "$type" (`String "app.bsky.embed.images")
      |> add "images" (`Array [|`Map record1_embed_images_0|]) )
  in
  let record1 : Dag_cbor.value =
    `Map
      String_map.(
        empty
        |> add "$type" (`String "app.bsky.feed.post")
        |> add "createdAt" (`String "2024-08-13T01:16:06.453Z")
        |> add "langs" (`Array [|`String "en"|])
        |> add "text" (`String "exclusively on bluesky")
        |> add "embed" (`Map record1_embed) )
  in
  let encoded1 = Dag_cbor.encode record1 in
  Alcotest.(check string)
    "atproto record 1 encodes correctly"
    "bafyreicbb3p4hmtm7iw3k7kiydzqp7qhufq3jdc5sbc4gxa4mxqd6bywba"
    Cid.(create Dcbor encoded1 |> to_string) ;
  let record2_embed_images_0_aspect_ratio : Dag_cbor.value String_map.t =
    String_map.(
      empty |> add "height" (`Integer 2000L) |> add "width" (`Integer 1500L) )
  in
  let record2_embed_images_0_image : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "$type" (`String "blob")
      |> add "ref"
           (`Link
              ( Result.get_ok
              @@ Cid.of_string
                   "bafkreibdqy5qcefkcuvopnkt2tip5wzouscmp6duz377cneknktnsgfewe"
              ) )
      |> add "mimeType" (`String "image/jpeg")
      |> add "size" (`Integer 531257L) )
  in
  let record2_embed_images_0 : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "alt" (`String "")
      |> add "aspectRatio" (`Map record2_embed_images_0_aspect_ratio)
      |> add "image" (`Map record2_embed_images_0_image) )
  in
  let record2_embed : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "$type" (`String "app.bsky.embed.images")
      |> add "images" (`Array [|`Map record2_embed_images_0|]) )
  in
  let record2_facets_0 : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "features"
           (`Array
              [| `Map
                   ( String_map.empty
                   |> String_map.add "$type"
                        (`String "app.bsky.richtext.facet#tag")
                   |> String_map.add "tag" (`String "写真") ) |] )
      |> add "index"
           (`Map
              ( String_map.empty
              |> String_map.add "byteEnd" (`Integer 109L)
              |> String_map.add "byteStart" (`Integer 100L) ) ) )
  in
  let record2_facets_1 : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "features"
           (`Array
              [| `Map
                   ( String_map.empty
                   |> String_map.add "$type"
                        (`String "app.bsky.richtext.facet#tag")
                   |> String_map.add "tag" (`String "日の出") ) |] )
      |> add "index"
           (`Map
              ( String_map.empty
              |> String_map.add "byteEnd" (`Integer 122L)
              |> String_map.add "byteStart" (`Integer 110L) ) ) )
  in
  let record2_facets_2 : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "features"
           (`Array
              [| `Map
                   ( String_map.empty
                   |> String_map.add "$type"
                        (`String "app.bsky.richtext.facet#tag")
                   |> String_map.add "tag" (`String "日常") ) |] )
      |> add "index"
           (`Map
              ( String_map.empty
              |> String_map.add "byteEnd" (`Integer 132L)
              |> String_map.add "byteStart" (`Integer 123L) ) ) )
  in
  let record2_facets_3 : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "features"
           (`Array
              [| `Map
                   ( String_map.empty
                   |> String_map.add "$type"
                        (`String "app.bsky.richtext.facet#tag")
                   |> String_map.add "tag" (`String "キリトリセカイ") ) |] )
      |> add "index"
           (`Map
              ( String_map.empty
              |> String_map.add "byteEnd" (`Integer 157L)
              |> String_map.add "byteStart" (`Integer 133L) ) ) )
  in
  let record2_facets =
    [| `Map record2_facets_0
     ; `Map record2_facets_1
     ; `Map record2_facets_2
     ; `Map record2_facets_3 |]
  in
  let record2_map : Dag_cbor.value String_map.t =
    String_map.(
      empty
      |> add "$type" (`String "app.bsky.feed.post")
      |> add "createdAt" (`String "2025-01-02T23:29:41.149Z")
      |> add "langs" (`Array [|`String "ja"|])
      |> add "text"
           (`String
              "おはようございます☀️\n今日の日の出です\n寒かったけど綺麗でしたよ✨\n\n＃写真\n＃日の出\n＃日常\n＃キリトリセカイ"
           )
      |> add "embed" (`Map record2_embed)
      |> add "facets" (`Array record2_facets) )
  in
  let record2 = `Map record2_map in
  let encoded2 = Dag_cbor.encode record2 in
  Alcotest.(check string)
    "atproto record 2 encodes correctly"
    "bafyreiarjuvb3oppjnouaiasitt2tekkhhge6qsd4xegutblzgmihmnrhi"
    Cid.(create Dcbor encoded2 |> to_string)

let test_invalid_numbers () =
  Alcotest.check_raises "encode rejects out of range positive integer"
    (Invalid_argument
       "write_integer: value out of range ([-9007199254740991, \
        9007199254740991])" ) (fun () ->
      ignore (Dag_cbor.encode (`Integer 9007199254740992L)) ) ;
  Alcotest.check_raises "encode rejects out of range negative integer"
    (Invalid_argument
       "write_integer: value out of range ([-9007199254740991, \
        9007199254740991])" ) (fun () ->
      ignore (Dag_cbor.encode (`Integer (-9007199254740992L))) )

let test_decode_multiple_objects () =
  let obj1 = `Map (String_map.add "foo" (`Boolean true) String_map.empty) in
  let obj2 = `Map (String_map.add "bar" (`Boolean false) String_map.empty) in
  let encoded1 = Dag_cbor.encode obj1 in
  let encoded2 = Dag_cbor.encode obj2 in
  let combined = Bytes.create (Bytes.length encoded1 + Bytes.length encoded2) in
  Bytes.blit encoded1 0 combined 0 (Bytes.length encoded1) ;
  Bytes.blit encoded2 0 combined (Bytes.length encoded1) (Bytes.length encoded2) ;
  let decoded1, remainder = Dag_cbor.Decoder.decode_first combined in
  Alcotest.(check bool) "first object decoded correctly" true (decoded1 = obj1) ;
  let decoded2, final_remainder = Dag_cbor.Decoder.decode_first remainder in
  Alcotest.(check bool) "second object decoded correctly" true (decoded2 = obj2) ;
  Alcotest.(check int) "no remaining bytes" 0 (Bytes.length final_remainder)

let test_yojson_roundtrip () =
  let record_embed_images_0_aspect_ratio : Yojson.Safe.t =
    `Assoc [("height", `Int 885); ("width", `Int 665)]
  in
  let record_embed_images_0_image : Yojson.Safe.t =
    `Assoc
      [ ("height", `Int 885)
      ; ("width", `Int 665)
      ; ("mimeType", `String "image/jpeg")
      ; ("size", `Int 645553) ]
  in
  let record_embed_images_0 : Yojson.Safe.t =
    `Assoc
      [ ( "alt"
        , `String
            "a photoshopped picture of kit with a microphone. kit is saying \
             \"meow\"" )
      ; ("aspectRatio", record_embed_images_0_aspect_ratio)
      ; ("image", record_embed_images_0_image) ]
  in
  let record_embed : Yojson.Safe.t =
    `Assoc
      [ ("$type", `String "app.bsky.embed.images")
      ; ("images", `List [record_embed_images_0]) ]
  in
  let record : Yojson.Safe.t =
    `Assoc
      [ ("$type", `String "app.bsky.feed.post")
      ; ("createdAt", `String "2024-08-13T01:16:06.453Z")
      ; ("langs", `List [`String "en"])
      ; ("text", `String "exclusively on bluesky")
      ; ("embed", record_embed) ]
  in
  let encoded = Dag_cbor.encode_yojson record in
  let decoded = Dag_cbor.decode_to_yojson encoded in
  Alcotest.(check yojson_testable) "yojson roundtrip" record decoded

let () =
  Alcotest.run "dag-cbor"
    [ ( "dag-cbor encoding"
      , [ ("encode_primitives", `Quick, test_encode_primitives)
        ; ("round_trip", `Quick, test_round_trip)
        ; ("atproto_records", `Quick, test_atproto_post_records)
        ; ("invalid_numbers", `Quick, test_invalid_numbers)
        ; ("decode_multiple", `Quick, test_decode_multiple_objects)
        ; ("yojson_roundtrip", `Quick, test_yojson_roundtrip) ] ) ]
