open Alcotest
open Hermes_cli.Naming

(** helpers *)
let test_string = testable Fmt.string String.equal

let test_camel_to_snake_simple () =
  check test_string "simple camelCase" "first_name" (camel_to_snake "firstName")

let test_camel_to_snake_single () =
  check test_string "single word" "name" (camel_to_snake "name")

let test_camel_to_snake_already_snake () =
  check test_string "already snake_case" "first_name"
    (camel_to_snake "first_name")

let test_camel_to_snake_multiple_caps () =
  check test_string "multiple caps" "auth_factor_token"
    (camel_to_snake "authFactorToken")

let test_camel_to_snake_leading_cap () =
  check test_string "leading capital" "name" (camel_to_snake "Name")

let test_camel_to_snake_all_caps () =
  check test_string "all caps sequence" "d_i_d" (camel_to_snake "DID")

let test_camel_to_snake_empty () =
  check test_string "empty string" "" (camel_to_snake "")

let test_is_reserved_type () =
  check bool "type is reserved" true (is_reserved "type")

let test_is_reserved_module () =
  check bool "module is reserved" true (is_reserved "module")

let test_is_reserved_and () =
  check bool "and is reserved" true (is_reserved "and")

let test_is_reserved_user () =
  check bool "user is not reserved" false (is_reserved "user")

let test_is_reserved_case_insensitive () =
  check bool "TYPE is reserved (case insensitive)" true (is_reserved "TYPE")

let test_escape_keyword_reserved () =
  check test_string "escapes type" "type_" (escape_keyword "type")

let test_escape_keyword_not_reserved () =
  check test_string "does not escape user" "user" (escape_keyword "user")

let test_escape_keyword_module () =
  check test_string "escapes module" "module_" (escape_keyword "module")

let test_field_name_camel () =
  check test_string "converts camelCase" "first_name" (field_name "firstName")

let test_field_name_reserved () =
  check test_string "escapes reserved" "type_" (field_name "type")

let test_field_name_camel_reserved () =
  check test_string "converts and escapes" "to_" (field_name "to")

let test_module_name_simple () =
  check test_string "capitalizes" "App" (module_name_of_segment "app")

let test_module_name_already_cap () =
  check test_string "already capitalized" "App" (module_name_of_segment "App")

let test_module_name_empty () =
  check test_string "empty string" "" (module_name_of_segment "")

let test_module_path_simple () =
  check (list test_string) "simple path" ["App"; "Bsky"; "Graph"]
    (module_path_of_nsid "app.bsky.graph")

let test_module_path_single () =
  check (list test_string) "single segment" ["App"] (module_path_of_nsid "app")

let test_module_path_full () =
  check (list test_string) "full nsid"
    ["Com"; "Atproto"; "Server"; "CreateSession"]
    (module_path_of_nsid "com.atproto.server.createSession")

let test_type_name_of_nsid () =
  check test_string "extracts last segment" "get_profile"
    (type_name_of_nsid "app.bsky.actor.getProfile")

let test_type_name_simple () =
  check test_string "converts name" "invite_code" (type_name "inviteCode")

let test_type_name_reserved () =
  check test_string "escapes reserved" "type_" (type_name "type")

let test_def_module_name () =
  check test_string "capitalizes" "InviteCode" (def_module_name "inviteCode")

let test_variant_local_ref () =
  check test_string "local ref" "Relationship"
    (variant_name_of_ref "#relationship")

let test_variant_external_ref () =
  check test_string "external ref" "SomeDef"
    (variant_name_of_ref "com.example.defs#someDef")

let test_variant_just_nsid () =
  check test_string "just nsid" "Defs" (variant_name_of_ref "com.example.defs")

let test_flat_module_name () =
  check test_string "flat name" "Com_atproto_server_defs"
    (flat_module_name_of_nsid "com.atproto.server.defs")

let test_flat_module_name_short () =
  check test_string "short nsid" "App_bsky"
    (flat_module_name_of_nsid "app.bsky")

let test_file_path () =
  check test_string "file path" "com_atproto_server_defs.ml"
    (file_path_of_nsid "com.atproto.server.defs")

let test_key_annotation_needed () =
  check test_string "annotation needed" " [@key \"firstName\"]"
    (key_annotation "firstName" "first_name")

let test_key_annotation_not_needed () =
  check test_string "annotation not needed" "" (key_annotation "name" "name")

let camel_to_snake_tests =
  [ ("simple camelCase", `Quick, test_camel_to_snake_simple)
  ; ("single word", `Quick, test_camel_to_snake_single)
  ; ("already snake_case", `Quick, test_camel_to_snake_already_snake)
  ; ("multiple caps", `Quick, test_camel_to_snake_multiple_caps)
  ; ("leading capital", `Quick, test_camel_to_snake_leading_cap)
  ; ("all caps", `Quick, test_camel_to_snake_all_caps)
  ; ("empty string", `Quick, test_camel_to_snake_empty) ]

let is_reserved_tests =
  [ ("type is reserved", `Quick, test_is_reserved_type)
  ; ("module is reserved", `Quick, test_is_reserved_module)
  ; ("and is reserved", `Quick, test_is_reserved_and)
  ; ("user is not reserved", `Quick, test_is_reserved_user)
  ; ("case insensitive", `Quick, test_is_reserved_case_insensitive) ]

let escape_keyword_tests =
  [ ("escapes reserved", `Quick, test_escape_keyword_reserved)
  ; ("does not escape non-reserved", `Quick, test_escape_keyword_not_reserved)
  ; ("escapes module", `Quick, test_escape_keyword_module) ]

let field_name_tests =
  [ ("converts camelCase", `Quick, test_field_name_camel)
  ; ("escapes reserved", `Quick, test_field_name_reserved)
  ; ("converts and escapes", `Quick, test_field_name_camel_reserved) ]

let module_name_tests =
  [ ("capitalizes segment", `Quick, test_module_name_simple)
  ; ("already capitalized", `Quick, test_module_name_already_cap)
  ; ("empty string", `Quick, test_module_name_empty)
  ; ("module path simple", `Quick, test_module_path_simple)
  ; ("module path single", `Quick, test_module_path_single)
  ; ("module path full", `Quick, test_module_path_full) ]

let type_name_tests =
  [ ("type_name_of_nsid", `Quick, test_type_name_of_nsid)
  ; ("type_name simple", `Quick, test_type_name_simple)
  ; ("type_name reserved", `Quick, test_type_name_reserved)
  ; ("def_module_name", `Quick, test_def_module_name) ]

let variant_name_tests =
  [ ("local ref", `Quick, test_variant_local_ref)
  ; ("external ref", `Quick, test_variant_external_ref)
  ; ("just nsid", `Quick, test_variant_just_nsid) ]

let flat_module_tests =
  [ ("flat module name", `Quick, test_flat_module_name)
  ; ("flat module short", `Quick, test_flat_module_name_short)
  ; ("file path", `Quick, test_file_path) ]

let annotation_tests =
  [ ("annotation needed", `Quick, test_key_annotation_needed)
  ; ("annotation not needed", `Quick, test_key_annotation_not_needed) ]

let () =
  run "Naming"
    [ ("camel_to_snake", camel_to_snake_tests)
    ; ("is_reserved", is_reserved_tests)
    ; ("escape_keyword", escape_keyword_tests)
    ; ("field_name", field_name_tests)
    ; ("module_name", module_name_tests)
    ; ("type_name", type_name_tests)
    ; ("variant_name", variant_name_tests)
    ; ("flat_module", flat_module_tests)
    ; ("annotations", annotation_tests) ]
