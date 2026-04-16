open Alcotest
open Pegasus
module Types = Hermes_cli.Lexicon_types

let test_is_did () =
  check bool "valid plc" true
    (Record_validator.is_did "did:plc:vwzwgnygau7ed7b7wt5ux7y2") ;
  check bool "valid web" true (Record_validator.is_did "did:web:example.com") ;
  check bool "missing method" false (Record_validator.is_did "did::abc") ;
  check bool "missing id" false (Record_validator.is_did "did:plc:") ;
  check bool "no prefix" false
    (Record_validator.is_did "plc:vwzwgnygau7ed7b7wt5ux7y2") ;
  check bool "upper method" false
    (Record_validator.is_did "did:PLC:vwzwgnygau7ed7b7wt5ux7y2")

let test_is_handle () =
  check bool "simple" true (Record_validator.is_handle "alice.bsky.social") ;
  check bool "short tld" true (Record_validator.is_handle "a.co") ;
  check bool "digits ok in middle" true
    (Record_validator.is_handle "user1.example.com") ;
  check bool "no dots" false (Record_validator.is_handle "handle") ;
  check bool "trailing dot" false (Record_validator.is_handle "alice.bsky.") ;
  check bool "numeric tld" false (Record_validator.is_handle "alice.bsky.1") ;
  check bool "leading dash" false
    (Record_validator.is_handle "-bad.example.com")

let test_is_nsid () =
  check bool "valid" true
    (Record_validator.is_nsid "com.atproto.repo.createRecord") ;
  check bool "app.bsky" true (Record_validator.is_nsid "app.bsky.feed.post") ;
  check bool "too few labels" false (Record_validator.is_nsid "com.example") ;
  check bool "name starts with digit" false
    (Record_validator.is_nsid "com.example.1bad") ;
  check bool "name contains underscore" false
    (Record_validator.is_nsid "com.example.bad_name")

let test_is_tid () =
  check bool "valid" true (Record_validator.is_tid "3jzfcijpj2z2a") ;
  check bool "wrong length" false (Record_validator.is_tid "3jzfc") ;
  check bool "invalid first char" false
    (Record_validator.is_tid "kjzfcijpj2z2a")

let test_is_record_key () =
  check bool "self" true (Record_validator.is_record_key "self") ;
  check bool "with dash" true (Record_validator.is_record_key "some-key.v1") ;
  check bool "dot not ok" false (Record_validator.is_record_key ".") ;
  check bool "dotdot not ok" false (Record_validator.is_record_key "..") ;
  check bool "slash forbidden" false (Record_validator.is_record_key "a/b")

let test_is_datetime () =
  check bool "with ms" true
    (Record_validator.is_datetime "1985-04-12T23:20:50.123Z") ;
  check bool "no ms" true (Record_validator.is_datetime "2024-01-01T00:00:00Z") ;
  check bool "offset" true
    (Record_validator.is_datetime "2024-01-01T00:00:00+05:00") ;
  check bool "no tz" false (Record_validator.is_datetime "2024-01-01T00:00:00")

let test_count_graphemes () =
  check int "ascii" 5 (Record_validator.count_graphemes "hello") ;
  check int "empty" 0 (Record_validator.count_graphemes "") ;
  check int "e-acute precomposed" 4
    (Record_validator.count_graphemes "caf\xC3\xA9") ;
  (* "cafe" + combining acute U+0301: 5 codepoints, 4 clusters *)
  check int "e-acute decomposed" 4
    (Record_validator.count_graphemes "cafe\xCC\x81") ;
  check int "camel emoji" 1
    (Record_validator.count_graphemes "\xF0\x9F\x90\xAB") ;
  (* ZWJ family: man + ZWJ + woman = 3 codepoints, 1 cluster *)
  check int "zwj sequence" 1
    (Record_validator.count_graphemes
       "\xF0\x9F\x91\xA8\xE2\x80\x8D\xF0\x9F\x91\xA9" )

let setup_schema_cache nsid json_str =
  let doc =
    Hermes_cli.Parser.parse_lexicon_doc (Yojson.Safe.from_string json_str)
  in
  Ttl_cache.String_cache.set Lexicon_resolver.schema_cache nsid doc

let validate_json ~nsid json_str =
  let json = Yojson.Safe.from_string json_str in
  Record_validator.validate_record ~nsid ~record:json

let check_ok msg result =
  match result with
  | Ok () ->
      ()
  | Error e ->
      failf "%s: expected Ok, got Error: %s" msg e

let check_err msg_fragment result =
  match result with
  | Ok () ->
      fail "expected error, got Ok"
  | Error e ->
      if
        String.length msg_fragment > 0
        && (not (Str.string_match (Str.regexp_string msg_fragment) e 0))
        && not
             ( try
                 ignore
                   (Str.search_forward (Str.regexp_string msg_fragment) e 0) ;
                 true
               with Not_found -> false )
      then failf "error %S did not contain %S" e msg_fragment

let simple_post_lexicon =
  {|{
    "lexicon": 1,
    "id": "com.test.post",
    "defs": {
      "main": {
        "type": "record",
        "key": "tid",
        "record": {
          "type": "object",
          "required": ["text", "createdAt"],
          "properties": {
            "text": {
              "type": "string",
              "maxLength": 300,
              "maxGraphemes": 100
            },
            "createdAt": { "type": "string", "format": "datetime" },
            "likes": { "type": "integer", "minimum": 0, "maximum": 1000000 },
            "pinned": { "type": "boolean" }
          }
        }
      }
    }
  }|}

let test_string_length_pass () =
  setup_schema_cache "com.test.post" simple_post_lexicon ;
  let result =
    Lwt_main.run
      (validate_json ~nsid:"com.test.post"
         {|{"text":"hi","createdAt":"2024-01-01T00:00:00Z"}|} )
  in
  check_ok "valid record" result

let test_string_too_long () =
  setup_schema_cache "com.test.post" simple_post_lexicon ;
  let long_text = String.make 301 'x' in
  let body =
    Printf.sprintf {|{"text":"%s","createdAt":"2024-01-01T00:00:00Z"}|}
      long_text
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.post" body) in
  check_err "maxLength" result

let test_missing_required () =
  setup_schema_cache "com.test.post" simple_post_lexicon ;
  let result =
    Lwt_main.run (validate_json ~nsid:"com.test.post" {|{"text":"hi"}|})
  in
  check_err "createdAt" result

let test_integer_out_of_range () =
  setup_schema_cache "com.test.post" simple_post_lexicon ;
  let result =
    Lwt_main.run
      (validate_json ~nsid:"com.test.post"
         {|{"text":"hi","createdAt":"2024-01-01T00:00:00Z","likes":-1}|} )
  in
  check_err "minimum" result

let test_bad_datetime_format () =
  setup_schema_cache "com.test.post" simple_post_lexicon ;
  let result =
    Lwt_main.run
      (validate_json ~nsid:"com.test.post"
         {|{"text":"hi","createdAt":"not-a-date"}|} )
  in
  check_err "datetime" result

let test_wrong_type () =
  setup_schema_cache "com.test.post" simple_post_lexicon ;
  let result =
    Lwt_main.run
      (validate_json ~nsid:"com.test.post"
         {|{"text":42,"createdAt":"2024-01-01T00:00:00Z"}|} )
  in
  check_err "string" result

let test_extra_fields_ignored () =
  setup_schema_cache "com.test.post" simple_post_lexicon ;
  let result =
    Lwt_main.run
      (validate_json ~nsid:"com.test.post"
         {|{"text":"hi","createdAt":"2024-01-01T00:00:00Z","extra":"ok"}|} )
  in
  check_ok "extras allowed" result

(* record with nullable + array + cid-link + local ref *)
let ref_lexicon =
  {|{
    "lexicon": 1,
    "id": "com.test.ref",
    "defs": {
      "main": {
        "type": "record",
        "key": "tid",
        "record": {
          "type": "object",
          "required": ["ids", "author"],
          "nullable": ["bio"],
          "properties": {
            "ids": {
              "type": "array",
              "minLength": 1,
              "items": { "type": "string", "format": "cid" }
            },
            "author": { "type": "ref", "ref": "#person" },
            "bio": { "type": "string" },
            "icon": { "type": "cid-link" }
          }
        }
      },
      "person": {
        "type": "object",
        "required": ["did"],
        "properties": {
          "did": { "type": "string", "format": "did" },
          "name": { "type": "string", "maxLength": 50 }
        }
      }
    }
  }|}

let valid_cid = "bafkreibjfgx2gprinfvicegelk5kosd6y2frmqpqzwqkg7usac74l3t2v4"

let test_array_and_local_ref_ok () =
  setup_schema_cache "com.test.ref" ref_lexicon ;
  let body =
    Printf.sprintf
      {|{"ids":["%s"],"author":{"did":"did:plc:vwzwgnygau7ed7b7wt5ux7y2","name":"Alice"}}|}
      valid_cid
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.ref" body) in
  check_ok "valid ref" result

let test_array_min_length () =
  setup_schema_cache "com.test.ref" ref_lexicon ;
  let body =
    {|{"ids":[],"author":{"did":"did:plc:vwzwgnygau7ed7b7wt5ux7y2"}}|}
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.ref" body) in
  check_err "minLength" result

let test_cid_in_array_invalid () =
  setup_schema_cache "com.test.ref" ref_lexicon ;
  let body =
    {|{"ids":["not-a-cid"],"author":{"did":"did:plc:vwzwgnygau7ed7b7wt5ux7y2"}}|}
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.ref" body) in
  check_err "cid" result

let test_ref_missing_required () =
  setup_schema_cache "com.test.ref" ref_lexicon ;
  let body =
    Printf.sprintf {|{"ids":["%s"],"author":{"name":"nodid"}}|} valid_cid
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.ref" body) in
  check_err "did" result

let test_nullable_field_allowed () =
  setup_schema_cache "com.test.ref" ref_lexicon ;
  let body =
    Printf.sprintf
      {|{"ids":["%s"],"author":{"did":"did:plc:vwzwgnygau7ed7b7wt5ux7y2"},"bio":null}|}
      valid_cid
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.ref" body) in
  check_ok "null allowed" result

let test_cid_link_shape () =
  setup_schema_cache "com.test.ref" ref_lexicon ;
  let body =
    Printf.sprintf
      {|{"ids":["%s"],"author":{"did":"did:plc:vwzwgnygau7ed7b7wt5ux7y2"},"icon":{"$link":"%s"}}|}
      valid_cid valid_cid
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.ref" body) in
  check_ok "cid-link ok" result

let test_cid_link_bad () =
  setup_schema_cache "com.test.ref" ref_lexicon ;
  let body =
    Printf.sprintf
      {|{"ids":["%s"],"author":{"did":"did:plc:vwzwgnygau7ed7b7wt5ux7y2"},"icon":{"$link":"nope"}}|}
      valid_cid
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.ref" body) in
  check_err "CID" result

(* blob + union *)
let blob_lexicon =
  {|{
    "lexicon": 1,
    "id": "com.test.blob",
    "defs": {
      "main": {
        "type": "record",
        "key": "tid",
        "record": {
          "type": "object",
          "required": ["image"],
          "properties": {
            "image": {
              "type": "blob",
              "accept": ["image/*"],
              "maxSize": 1000
            }
          }
        }
      }
    }
  }|}

let test_blob_typed_ok () =
  setup_schema_cache "com.test.blob" blob_lexicon ;
  let body =
    Printf.sprintf
      {|{"image":{"$type":"blob","ref":{"$link":"%s"},"mimeType":"image/png","size":500}}|}
      valid_cid
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.blob" body) in
  check_ok "typed blob" result

let test_blob_mime_rejected () =
  setup_schema_cache "com.test.blob" blob_lexicon ;
  let body =
    Printf.sprintf
      {|{"image":{"$type":"blob","ref":{"$link":"%s"},"mimeType":"video/mp4","size":500}}|}
      valid_cid
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.blob" body) in
  check_err "accept" result

let test_blob_too_big () =
  setup_schema_cache "com.test.blob" blob_lexicon ;
  let body =
    Printf.sprintf
      {|{"image":{"$type":"blob","ref":{"$link":"%s"},"mimeType":"image/png","size":9999}}|}
      valid_cid
  in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.blob" body) in
  check_err "maxSize" result

let union_lexicon =
  {|{
    "lexicon": 1,
    "id": "com.test.union",
    "defs": {
      "main": {
        "type": "record",
        "key": "tid",
        "record": {
          "type": "object",
          "required": ["payload"],
          "properties": {
            "payload": {
              "type": "union",
              "refs": ["#foo", "#bar"],
              "closed": true
            }
          }
        }
      },
      "foo": {
        "type": "object",
        "required": ["a"],
        "properties": { "a": { "type": "string" } }
      },
      "bar": {
        "type": "object",
        "required": ["b"],
        "properties": { "b": { "type": "integer" } }
      }
    }
  }|}

let test_union_closed_match () =
  setup_schema_cache "com.test.union" union_lexicon ;
  let body = {|{"payload":{"$type":"com.test.union#foo","a":"hi"}}|} in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.union" body) in
  check_ok "closed union match" result

let test_union_closed_no_match () =
  setup_schema_cache "com.test.union" union_lexicon ;
  let body = {|{"payload":{"$type":"com.test.union#baz","a":"hi"}}|} in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.union" body) in
  check_err "closed union" result

let test_union_closed_missing_type () =
  setup_schema_cache "com.test.union" union_lexicon ;
  let body = {|{"payload":{"a":"hi"}}|} in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.union" body) in
  check_err "$type" result

let open_union_lexicon =
  {|{
    "lexicon": 1,
    "id": "com.test.openu",
    "defs": {
      "main": {
        "type": "record",
        "key": "tid",
        "record": {
          "type": "object",
          "required": ["x"],
          "properties": {
            "x": { "type": "union", "refs": ["#only"] }
          }
        }
      },
      "only": {
        "type": "object",
        "required": ["a"],
        "properties": { "a": { "type": "string" } }
      }
    }
  }|}

let test_union_open_unknown_ok () =
  setup_schema_cache "com.test.openu" open_union_lexicon ;
  let body = {|{"x":{"$type":"com.future.thing","whatever":123}}|} in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.openu" body) in
  check_ok "open union accepts unknown" result

let test_union_open_known_still_validated () =
  setup_schema_cache "com.test.openu" open_union_lexicon ;
  (* $type matches the known ref, but the inner field type is wrong *)
  let body = {|{"x":{"$type":"com.test.openu#only","a":42}}|} in
  let result = Lwt_main.run (validate_json ~nsid:"com.test.openu" body) in
  check_err "string" result

let () =
  run "record_validator"
    [ ( "formats"
      , [ ("is_did", `Quick, test_is_did)
        ; ("is_handle", `Quick, test_is_handle)
        ; ("is_nsid", `Quick, test_is_nsid)
        ; ("is_tid", `Quick, test_is_tid)
        ; ("is_record_key", `Quick, test_is_record_key)
        ; ("is_datetime", `Quick, test_is_datetime) ] )
    ; ("graphemes", [("count_graphemes", `Quick, test_count_graphemes)])
    ; ( "primitives"
      , [ ("string length pass", `Quick, test_string_length_pass)
        ; ("string too long", `Quick, test_string_too_long)
        ; ("missing required", `Quick, test_missing_required)
        ; ("integer out of range", `Quick, test_integer_out_of_range)
        ; ("bad datetime", `Quick, test_bad_datetime_format)
        ; ("wrong type", `Quick, test_wrong_type)
        ; ("extra fields ignored", `Quick, test_extra_fields_ignored) ] )
    ; ( "compound"
      , [ ("array + local ref", `Quick, test_array_and_local_ref_ok)
        ; ("array minLength", `Quick, test_array_min_length)
        ; ("cid in array invalid", `Quick, test_cid_in_array_invalid)
        ; ("ref missing required", `Quick, test_ref_missing_required)
        ; ("nullable allowed", `Quick, test_nullable_field_allowed)
        ; ("cid-link shape", `Quick, test_cid_link_shape)
        ; ("cid-link bad", `Quick, test_cid_link_bad) ] )
    ; ( "blob"
      , [ ("typed ok", `Quick, test_blob_typed_ok)
        ; ("mime rejected", `Quick, test_blob_mime_rejected)
        ; ("too big", `Quick, test_blob_too_big) ] )
    ; ( "union"
      , [ ("closed match", `Quick, test_union_closed_match)
        ; ("closed no match", `Quick, test_union_closed_no_match)
        ; ("closed missing $type", `Quick, test_union_closed_missing_type)
        ; ("open unknown ok", `Quick, test_union_open_unknown_ok)
        ; ( "open known still validated"
          , `Quick
          , test_union_open_known_still_validated ) ] ) ]
