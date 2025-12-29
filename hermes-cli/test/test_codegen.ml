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
  check bool "contains type result" true (contains code "type result =") ;
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
  check bool "output is string * string tuple" true
    (contains code "type output = string * string") ;
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
  ; ("key annotation", `Quick, test_gen_key_annotation) ]

let union_tests =
  [ ("open union", `Quick, test_gen_union_type)
  ; ("closed union", `Quick, test_gen_closed_union) ]

let xrpc_tests =
  [ ("query module", `Quick, test_gen_query_module)
  ; ("procedure module", `Quick, test_gen_procedure_module)
  ; ("query with bytes output", `Quick, test_gen_query_bytes_output)
  ; ("procedure with bytes input", `Quick, test_gen_procedure_bytes_input) ]

let ordering_tests = [("type ordering", `Quick, test_type_ordering)]

let token_tests = [("token generation", `Quick, test_gen_token)]

let () =
  run "Codegen"
    [ ("objects", object_tests)
    ; ("unions", union_tests)
    ; ("xrpc", xrpc_tests)
    ; ("ordering", ordering_tests)
    ; ("tokens", token_tests) ]
