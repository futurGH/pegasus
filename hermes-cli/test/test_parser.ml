open Alcotest
open Hermes_cli

(** helpers *)
let test_string = testable Fmt.string String.equal

(* parsing a simple object type *)
let test_parse_simple_object () =
  let json =
    {|{
    "lexicon": 1,
    "id": "com.example.test",
    "defs": {
      "main": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "count": {"type": "integer"}
        },
        "required": ["name"]
      }
    }
  }|}
  in
  match Parser.parse_string json with
  | Ok doc ->
      check test_string "id matches" "com.example.test" doc.id ;
      check int "lexicon version" 1 doc.lexicon ;
      check int "one definition" 1 (List.length doc.defs)
  | Error e ->
      fail ("parse failed: " ^ e)

(* parsing string type with constraints *)
let test_parse_string_type () =
  let json =
    {|{
    "lexicon": 1,
    "id": "com.example.string",
    "defs": {
      "main": {
        "type": "object",
        "properties": {
          "handle": {
            "type": "string",
            "format": "handle",
            "minLength": 3,
            "maxLength": 50
          }
        }
      }
    }
  }|}
  in
  match Parser.parse_string json with
  | Ok doc -> (
      check int "one definition" 1 (List.length doc.defs) ;
      let def = List.hd doc.defs in
      match def.type_def with
      | Lexicon_types.Object spec -> (
          check int "one property" 1 (List.length spec.properties) ;
          let _, prop = List.hd spec.properties in
          match prop.type_def with
          | Lexicon_types.String s ->
              check (option test_string) "format" (Some "handle") s.format ;
              check (option int) "minLength" (Some 3) s.min_length ;
              check (option int) "maxLength" (Some 50) s.max_length
          | _ ->
              fail "expected string type" )
      | _ ->
          fail "expected object type" )
  | Error e ->
      fail ("parse failed: " ^ e)

(* parsing array type *)
let test_parse_array_type () =
  let json =
    {|{
    "lexicon": 1,
    "id": "com.example.array",
    "defs": {
      "main": {
        "type": "object",
        "properties": {
          "items": {
            "type": "array",
            "items": {"type": "string"},
            "maxLength": 100
          }
        }
      }
    }
  }|}
  in
  match Parser.parse_string json with
  | Ok doc -> (
      let def = List.hd doc.defs in
      match def.type_def with
      | Lexicon_types.Object spec -> (
          let _, prop = List.hd spec.properties in
          match prop.type_def with
          | Lexicon_types.Array arr -> (
              check (option int) "maxLength" (Some 100) arr.max_length ;
              match arr.items with
              | Lexicon_types.String _ ->
                  ()
              | _ ->
                  fail "expected string items" )
          | _ ->
              fail "expected array type" )
      | _ ->
          fail "expected object type" )
  | Error e ->
      fail ("parse failed: " ^ e)

(* parsing ref type *)
let test_parse_ref_type () =
  let json =
    {|{
    "lexicon": 1,
    "id": "com.example.ref",
    "defs": {
      "main": {
        "type": "object",
        "properties": {
          "user": {
            "type": "ref",
            "ref": "com.example.defs#user"
          }
        }
      }
    }
  }|}
  in
  match Parser.parse_string json with
  | Ok doc -> (
      let def = List.hd doc.defs in
      match def.type_def with
      | Lexicon_types.Object spec -> (
          let _, prop = List.hd spec.properties in
          match prop.type_def with
          | Lexicon_types.Ref r ->
              check test_string "ref value" "com.example.defs#user" r.ref_
          | _ ->
              fail "expected ref type" )
      | _ ->
          fail "expected object type" )
  | Error e ->
      fail ("parse failed: " ^ e)

(* parsing union type *)
let test_parse_union_type () =
  let json =
    {|{
    "lexicon": 1,
    "id": "com.example.union",
    "defs": {
      "main": {
        "type": "union",
        "refs": ["#typeA", "#typeB"],
        "closed": true
      }
    }
  }|}
  in
  match Parser.parse_string json with
  | Ok doc -> (
      let def = List.hd doc.defs in
      match def.type_def with
      | Lexicon_types.Union u ->
          check int "two refs" 2 (List.length u.refs) ;
          check (option bool) "closed" (Some true) u.closed
      | _ ->
          fail "expected union type" )
  | Error e ->
      fail ("parse failed: " ^ e)

(* parsing query type *)
let test_parse_query_type () =
  let json =
    {|{
    "lexicon": 1,
    "id": "com.example.getUser",
    "defs": {
      "main": {
        "type": "query",
        "description": "Get a user",
        "parameters": {
          "type": "params",
          "properties": {
            "userId": {"type": "string"}
          },
          "required": ["userId"]
        },
        "output": {
          "encoding": "application/json",
          "schema": {
            "type": "object",
            "properties": {
              "name": {"type": "string"}
            }
          }
        }
      }
    }
  }|}
  in
  match Parser.parse_string json with
  | Ok doc -> (
      let def = List.hd doc.defs in
      match def.type_def with
      | Lexicon_types.Query q -> (
          check (option test_string) "description" (Some "Get a user")
            q.description ;
          ( match q.parameters with
          | Some params ->
              check int "one param" 1 (List.length params.properties)
          | None ->
              fail "expected parameters" ) ;
          match q.output with
          | Some output ->
              check test_string "encoding" "application/json" output.encoding
          | None ->
              fail "expected output" )
      | _ ->
          fail "expected query type" )
  | Error e ->
      fail ("parse failed: " ^ e)

(* parsing procedure type *)
let test_parse_procedure_type () =
  let json =
    {|{
    "lexicon": 1,
    "id": "com.example.createUser",
    "defs": {
      "main": {
        "type": "procedure",
        "input": {
          "encoding": "application/json",
          "schema": {
            "type": "object",
            "properties": {
              "name": {"type": "string"}
            },
            "required": ["name"]
          }
        },
        "output": {
          "encoding": "application/json",
          "schema": {
            "type": "object",
            "properties": {
              "id": {"type": "string"}
            }
          }
        }
      }
    }
  }|}
  in
  match Parser.parse_string json with
  | Ok doc -> (
      let def = List.hd doc.defs in
      match def.type_def with
      | Lexicon_types.Procedure p -> (
          ( match p.input with
          | Some input ->
              check test_string "input encoding" "application/json"
                input.encoding
          | None ->
              fail "expected input" ) ;
          match p.output with
          | Some output ->
              check test_string "output encoding" "application/json"
                output.encoding
          | None ->
              fail "expected output" )
      | _ ->
          fail "expected procedure type" )
  | Error e ->
      fail ("parse failed: " ^ e)

(* parsing invalid JSON *)
let test_parse_invalid_json () =
  let json = {|{ invalid json }|} in
  match Parser.parse_string json with
  | Ok _ ->
      fail "should have failed"
  | Error e ->
      check bool "has error message" true (String.length e > 0)

(* parsing missing required field *)
let test_parse_missing_field () =
  let json = {|{
    "lexicon": 1,
    "defs": {}
  }|} in
  match Parser.parse_string json with
  | Ok _ ->
      fail "should have failed (missing id)"
  | Error _ ->
      ()

(** tests *)

let object_tests =
  [ ("simple object", `Quick, test_parse_simple_object)
  ; ("string with constraints", `Quick, test_parse_string_type)
  ; ("array type", `Quick, test_parse_array_type)
  ; ("ref type", `Quick, test_parse_ref_type) ]

let complex_type_tests =
  [ ("union type", `Quick, test_parse_union_type)
  ; ("query type", `Quick, test_parse_query_type)
  ; ("procedure type", `Quick, test_parse_procedure_type) ]

let error_tests =
  [ ("invalid json", `Quick, test_parse_invalid_json)
  ; ("missing field", `Quick, test_parse_missing_field) ]

let () =
  run "Parser"
    [ ("objects", object_tests)
    ; ("complex_types", complex_type_tests)
    ; ("errors", error_tests) ]
