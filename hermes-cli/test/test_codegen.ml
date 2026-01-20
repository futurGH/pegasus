open Alcotest
open Hermes_cli

let contains s1 s2 =
  try
    let len = String.length s2 in
    for i = 0 to String.length s1 - len do
      if String.sub s1 i len = s2 then raise Exit
    done ;
    false
  with Exit -> true

(* create a simple lexicon doc for testing *)
let make_lexicon id defs =
  {Lexicon_types.lexicon= 1; id; revision= None; description= None; defs}

let make_def name type_def = {Lexicon_types.name; type_def}

let make_object_spec properties required =
  { Lexicon_types.properties
  ; required= Some required
  ; nullable= None
  ; description= None }

let make_property type_def = {Lexicon_types.type_def; description= None}

let string_type =
  Lexicon_types.String
    { format= None
    ; min_length= None
    ; max_length= None
    ; min_graphemes= None
    ; max_graphemes= None
    ; known_values= None
    ; enum= None
    ; const= None
    ; default= None
    ; description= None }

let int_type =
  Lexicon_types.Integer
    { minimum= None
    ; maximum= None
    ; enum= None
    ; const= None
    ; default= None
    ; description= None }

let[@warning "-32"] _bool_type =
  Lexicon_types.Boolean {const= None; default= None; description= None}

(* test generating a simple object type *)
let test_gen_simple_object () =
  let obj_spec =
    make_object_spec
      [("name", make_property string_type); ("age", make_property int_type)]
      ["name"; "age"]
  in
  let doc =
    make_lexicon "com.example.test"
      [make_def "main" (Lexicon_types.Object obj_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains type main" true (contains code "type main =") ;
  check bool "contains name field" true (contains code "name: string") ;
  check bool "contains age field" true (contains code "age: int") ;
  check bool "contains deriving" true (contains code "[@@deriving yojson")

(* test generating object with optional fields *)
let test_gen_optional_fields () =
  let obj_spec =
    make_object_spec
      [ ("required_field", make_property string_type)
      ; ("optional_field", make_property string_type) ]
      ["required_field"]
    (* only required_field is required *)
  in
  let doc =
    make_lexicon "com.example.optional"
      [make_def "main" (Lexicon_types.Object obj_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "required not option" true
    (contains code "required_field: string;") ;
  check bool "optional is option" true
    (contains code "optional_field: string option")

(* test generating with key annotation *)
let test_gen_key_annotation () =
  let obj_spec =
    make_object_spec [("firstName", make_property string_type)] ["firstName"]
  in
  let doc =
    make_lexicon "com.example.key"
      [make_def "main" (Lexicon_types.Object obj_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "has snake_case field" true (contains code "first_name:") ;
  check bool "has key annotation" true (contains code "[@key \"firstName\"]")

(* test generating union type *)
let test_gen_union_type () =
  let union_spec =
    { Lexicon_types.refs= ["#typeA"; "#typeB"]
    ; closed= Some false
    ; description= None }
  in
  let doc =
    make_lexicon "com.example.union"
      [make_def "result" (Lexicon_types.Union union_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains type result" true (contains code "type result_ =") ;
  check bool "contains TypeA variant" true (contains code "| TypeA of") ;
  check bool "contains TypeB variant" true (contains code "| TypeB of") ;
  check bool "contains Unknown (open)" true
    (contains code "| Unknown of Yojson.Safe.t")

(* test generating closed union *)
let test_gen_closed_union () =
  let union_spec =
    { Lexicon_types.refs= ["#typeA"; "#typeB"]
    ; closed= Some true
    ; description= None }
  in
  let doc =
    make_lexicon "com.example.closed"
      [make_def "result" (Lexicon_types.Union union_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "no Unknown variant" false (contains code "| Unknown of")

(* test generating query module *)
let test_gen_query_module () =
  let params_spec =
    { Lexicon_types.properties= [("userId", make_property string_type)]
    ; required= Some ["userId"]
    ; description= None }
  in
  let output_schema =
    Lexicon_types.Object
      (make_object_spec [("name", make_property string_type)] ["name"])
  in
  let output_body =
    { Lexicon_types.encoding= "application/json"
    ; schema= Some output_schema
    ; description= None }
  in
  let query_spec =
    { Lexicon_types.parameters= Some params_spec
    ; output= Some output_body
    ; errors= None
    ; description= Some "Get user by ID" }
  in
  let doc =
    make_lexicon "com.example.getUser"
      [make_def "main" (Lexicon_types.Query query_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains module Main" true (contains code "module Main = struct") ;
  check bool "contains nsid" true
    (contains code "let nsid = \"com.example.getUser\"") ;
  check bool "contains type params" true (contains code "type params =") ;
  check bool "contains type output" true (contains code "type output =") ;
  check bool "contains call function" true (contains code "let call") ;
  check bool "contains ~user_id param" true (contains code "~user_id") ;
  check bool "calls Hermes.query" true (contains code "Hermes.query")

(* test generating procedure module *)
let test_gen_procedure_module () =
  let input_schema =
    Lexicon_types.Object
      (make_object_spec
         [ ("name", make_property string_type)
         ; ("email", make_property string_type) ]
         ["name"; "email"] )
  in
  let input_body =
    { Lexicon_types.encoding= "application/json"
    ; schema= Some input_schema
    ; description= None }
  in
  let output_schema =
    Lexicon_types.Object
      (make_object_spec [("id", make_property string_type)] ["id"])
  in
  let output_body =
    { Lexicon_types.encoding= "application/json"
    ; schema= Some output_schema
    ; description= None }
  in
  let proc_spec =
    { Lexicon_types.parameters= None
    ; input= Some input_body
    ; output= Some output_body
    ; errors= None
    ; description= Some "Create user" }
  in
  let doc =
    make_lexicon "com.example.createUser"
      [make_def "main" (Lexicon_types.Procedure proc_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains module Main" true (contains code "module Main = struct") ;
  check bool "contains type input" true (contains code "type input =") ;
  check bool "contains type output" true (contains code "type output =") ;
  check bool "contains call function" true (contains code "let call") ;
  check bool "contains ~name param" true (contains code "~name") ;
  check bool "contains ~email param" true (contains code "~email") ;
  check bool "calls Hermes.procedure" true (contains code "Hermes.procedure")

(* test type ordering with dependencies *)
let test_type_ordering () =
  (* create types where typeB depends on typeA *)
  let type_a_spec =
    make_object_spec [("value", make_property string_type)] ["value"]
  in
  let type_b_spec =
    make_object_spec
      [ ( "a"
        , make_property (Lexicon_types.Ref {ref_= "#typeA"; description= None})
        ) ]
      ["a"]
  in
  let doc =
    make_lexicon "com.example.order"
      [ make_def "typeB" (Lexicon_types.Object type_b_spec)
      ; make_def "typeA" (Lexicon_types.Object type_a_spec) ]
  in
  let code = Codegen.gen_lexicon_module doc in
  (* typeA should appear before typeB in the generated code *)
  let pos_a =
    try Some (Str.search_forward (Str.regexp "type type_a") code 0)
    with Not_found -> None
  in
  let pos_b =
    try Some (Str.search_forward (Str.regexp "type type_b") code 0)
    with Not_found -> None
  in
  match (pos_a, pos_b) with
  | Some a, Some b ->
      check bool "typeA before typeB" true (a < b)
  | _ ->
      fail "both types should be present"

(* test generating token *)
let test_gen_token () =
  let token_spec : Lexicon_types.token_spec =
    {description= Some "A token value"}
  in
  let doc =
    make_lexicon "com.example.tokens"
      [make_def "myToken" (Lexicon_types.Token token_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains let my_token" true (contains code "let my_token =") ;
  check bool "contains full URI" true
    (contains code "com.example.tokens#myToken")

(* test generating inline union (union as property type) *)
let test_gen_inline_union () =
  let union_type =
    Lexicon_types.Union
      {refs= ["#typeA"; "#typeB"]; closed= Some false; description= None}
  in
  let obj_spec =
    make_object_spec [("status", make_property union_type)] ["status"]
  in
  let doc =
    make_lexicon "com.example.inline"
      [make_def "main" (Lexicon_types.Object obj_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  (* inline union should get its own type named after the property *)
  check bool "contains type status" true (contains code "type status =") ;
  check bool "contains TypeA variant" true (contains code "| TypeA of") ;
  check bool "contains TypeB variant" true (contains code "| TypeB of") ;
  (* main type should reference the inline union *)
  check bool "main uses status type" true (contains code "status: status")

(* test generating inline union in array (field_item context) *)
let test_gen_inline_union_in_array () =
  let union_type =
    Lexicon_types.Union
      {refs= ["#typeA"; "#typeB"]; closed= Some true; description= None}
  in
  let array_type =
    Lexicon_types.Array
      {items= union_type; min_length= None; max_length= None; description= None}
  in
  let obj_spec =
    make_object_spec [("items", make_property array_type)] ["items"]
  in
  let doc =
    make_lexicon "com.example.arrayunion"
      [make_def "main" (Lexicon_types.Object obj_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  (* inline union in array should be named field_item *)
  check bool "contains type items_item" true (contains code "type items_item =") ;
  check bool "items is items_item list" true (contains code "items_item list")

(* test generating empty object as unit *)
let test_gen_empty_object () =
  let empty_spec =
    { Lexicon_types.properties= []
    ; required= None
    ; nullable= None
    ; description= None }
  in
  let doc =
    make_lexicon "com.example.empty"
      [make_def "main" (Lexicon_types.Object empty_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains type main = unit" true (contains code "type main = unit") ;
  check bool "contains main_of_yojson _ = Ok ()" true
    (contains code "main_of_yojson _ = Ok ()")

(* test generating nullable fields (different from optional) *)
let test_gen_nullable_fields () =
  let obj_spec =
    { Lexicon_types.properties=
        [ ("required_nullable", make_property string_type)
        ; ("required_not_nullable", make_property string_type) ]
    ; required= Some ["required_nullable"; "required_not_nullable"]
    ; nullable= Some ["required_nullable"]
    ; description= None }
  in
  let doc =
    make_lexicon "com.example.nullable"
      [make_def "main" (Lexicon_types.Object obj_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  (* required + nullable = option *)
  check bool "nullable is option" true
    (contains code "required_nullable: string option") ;
  (* required + not nullable = not option *)
  check bool "not nullable is not option" true
    (contains code "required_not_nullable: string;")

(* test generating mutually recursive types *)
let test_gen_mutually_recursive () =
  (* typeA has a field of typeB, typeB has a field of typeA *)
  let type_a_spec =
    make_object_spec
      [ ("name", make_property string_type)
      ; ( "b"
        , make_property (Lexicon_types.Ref {ref_= "#typeB"; description= None})
        ) ]
      ["name"]
  in
  let type_b_spec =
    make_object_spec
      [ ("value", make_property int_type)
      ; ( "a"
        , make_property (Lexicon_types.Ref {ref_= "#typeA"; description= None})
        ) ]
      ["value"]
  in
  let doc =
    make_lexicon "com.example.recursive"
      [ make_def "typeA" (Lexicon_types.Object type_a_spec)
      ; make_def "typeB" (Lexicon_types.Object type_b_spec) ]
  in
  let code = Codegen.gen_lexicon_module doc in
  (* should use "type ... and ..." syntax *)
  check bool "has type keyword" true (contains code "type type_a =") ;
  check bool "has and keyword" true (contains code "and type_b =") ;
  (* deriving should appear after the last type in the group *)
  check bool "has deriving after and block" true
    (contains code "[@@deriving yojson")

(* test generating record type *)
let test_gen_record () =
  let record_spec : Lexicon_types.record_spec =
    { key= "tid"
    ; record= make_object_spec [("text", make_property string_type)] ["text"]
    ; description= Some "A simple record" }
  in
  let doc =
    make_lexicon "com.example.record"
      [make_def "main" (Lexicon_types.Record record_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains type main" true (contains code "type main =") ;
  check bool "contains text field" true (contains code "text: string")

(* test generating external ref *)
let test_gen_external_ref () =
  let obj_spec =
    make_object_spec
      [ ( "user"
        , make_property
            (Lexicon_types.Ref {ref_= "com.other.defs#user"; description= None})
        ) ]
      ["user"]
  in
  let doc =
    make_lexicon "com.example.extref"
      [make_def "main" (Lexicon_types.Object obj_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  (* should generate qualified module reference *)
  check bool "contains qualified ref" true (contains code "Com_other_defs.user")

(* test generating string type with known values *)
let test_gen_string_known_values () =
  let string_spec : Lexicon_types.string_spec =
    { format= None
    ; min_length= None
    ; max_length= None
    ; min_graphemes= None
    ; max_graphemes= None
    ; known_values= Some ["pending"; "active"; "completed"]
    ; enum= None
    ; const= None
    ; default= None
    ; description= Some "Status values" }
  in
  let doc =
    make_lexicon "com.example.status"
      [make_def "status" (Lexicon_types.String string_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains type status = string" true
    (contains code "type status = string") ;
  check bool "contains status_of_yojson" true (contains code "status_of_yojson")

(* test generating permission-set module *)
let test_gen_permission_set () =
  let perm1 : Lexicon_types.lex_permission =
    { resource= "rpc"
    ; extra=
        [("lxm", `List [`String "com.example.foo"]); ("inheritAud", `Bool true)]
    }
  in
  let perm2 : Lexicon_types.lex_permission =
    { resource= "repo"
    ; extra= [("collection", `List [`String "com.example.data"])] }
  in
  let ps_spec : Lexicon_types.permission_set_spec =
    { title= Some "Test Permissions"
    ; title_lang= Some [("de", "Test Berechtigungen")]
    ; detail= Some "Access to test features"
    ; detail_lang= None
    ; permissions= [perm1; perm2]
    ; description= None }
  in
  let doc =
    make_lexicon "com.example.perms"
      [make_def "main" (Lexicon_types.PermissionSet ps_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains type permission" true (contains code "type permission =") ;
  check bool "contains resource field" true (contains code "resource: string") ;
  check bool "contains lxm field" true (contains code "lxm: string list option") ;
  check bool "contains inherit_aud field" true
    (contains code "inherit_aud: bool option") ;
  check bool "contains type main" true (contains code "type main =") ;
  check bool "contains title field" true (contains code "title: string option") ;
  check bool "contains permissions field" true
    (contains code "permissions: permission list") ;
  check bool "contains deriving" true (contains code "[@@deriving yojson")

(* test generating query with bytes output (like getBlob) *)
let test_gen_query_bytes_output () =
  let params_spec =
    { Lexicon_types.properties=
        [("did", make_property string_type); ("cid", make_property string_type)]
    ; required= Some ["did"; "cid"]
    ; description= None }
  in
  let output_body =
    { Lexicon_types.encoding= "*/*" (* bytes output *)
    ; schema= None
    ; description= None }
  in
  let query_spec =
    { Lexicon_types.parameters= Some params_spec
    ; output= Some output_body
    ; errors= None
    ; description= Some "Get a blob" }
  in
  let doc =
    make_lexicon "com.atproto.sync.getBlob"
      [make_def "main" (Lexicon_types.Query query_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains module Main" true (contains code "module Main = struct") ;
  check bool "output is bytes * string tuple" true
    (contains code "type output = bytes * string") ;
  check bool "calls Hermes.query_bytes" true
    (contains code "Hermes.query_bytes")

(* test generating procedure with bytes input (like importRepo) *)
let test_gen_procedure_bytes_input () =
  let input_body =
    { Lexicon_types.encoding= "application/vnd.ipld.car" (* bytes input *)
    ; schema= None
    ; description= None }
  in
  let proc_spec =
    { Lexicon_types.parameters= None
    ; input= Some input_body
    ; output= None
    ; errors= None
    ; description= Some "Import a repo" }
  in
  let doc =
    make_lexicon "com.atproto.repo.importRepo"
      [make_def "main" (Lexicon_types.Procedure proc_spec)]
  in
  let code = Codegen.gen_lexicon_module doc in
  check bool "contains module Main" true (contains code "module Main = struct") ;
  check bool "has ?input param" true (contains code "?input") ;
  check bool "calls Hermes.procedure_bytes" true
    (contains code "Hermes.procedure_bytes") ;
  check bool "has content_type" true (contains code "application/vnd.ipld.car")

(** tests *)

let object_tests =
  [ ("simple object", `Quick, test_gen_simple_object)
  ; ("optional fields", `Quick, test_gen_optional_fields)
  ; ("key annotation", `Quick, test_gen_key_annotation)
  ; ("empty object", `Quick, test_gen_empty_object)
  ; ("nullable fields", `Quick, test_gen_nullable_fields)
  ; ("external ref", `Quick, test_gen_external_ref)
  ; ("record type", `Quick, test_gen_record) ]

let union_tests =
  [ ("open union", `Quick, test_gen_union_type)
  ; ("closed union", `Quick, test_gen_closed_union)
  ; ("inline union", `Quick, test_gen_inline_union)
  ; ("inline union in array", `Quick, test_gen_inline_union_in_array) ]

let xrpc_tests =
  [ ("query module", `Quick, test_gen_query_module)
  ; ("procedure module", `Quick, test_gen_procedure_module)
  ; ("query with bytes output", `Quick, test_gen_query_bytes_output)
  ; ("procedure with bytes input", `Quick, test_gen_procedure_bytes_input) ]

let ordering_tests =
  [ ("type ordering", `Quick, test_type_ordering)
  ; ("mutually recursive", `Quick, test_gen_mutually_recursive) ]

let token_tests = [("token generation", `Quick, test_gen_token)]

let string_tests =
  [("string with known values", `Quick, test_gen_string_known_values)]

let permission_set_tests =
  [("generate permission-set", `Quick, test_gen_permission_set)]

let () =
  run "Codegen"
    [ ("objects", object_tests)
    ; ("unions", union_tests)
    ; ("xrpc", xrpc_tests)
    ; ("ordering", ordering_tests)
    ; ("tokens", token_tests)
    ; ("strings", string_tests)
    ; ("permission-set", permission_set_tests) ]
