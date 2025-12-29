open Alcotest

let loc = Location.none

let test_nsid_to_module_path_simple () =
  let result = Hermes_ppx.nsid_to_module_path "app.bsky.graph" in
  check (list string) "simple path" ["App"; "Bsky"; "Graph"] result

let test_nsid_to_module_path_camel_case () =
  let result =
    Hermes_ppx.nsid_to_module_path "app.bsky.graph.getRelationships"
  in
  check (list string) "camelCase"
    ["App"; "Bsky"; "Graph"; "GetRelationships"]
    result

let test_nsid_to_module_path_single () =
  let result = Hermes_ppx.nsid_to_module_path "test" in
  check (list string) "single segment" ["Test"] result

let test_build_module_path_single () =
  let result = Hermes_ppx.build_module_path ~loc ["App"] in
  check string "single module" "App" (Ppxlib.Longident.name result.txt)

let test_build_module_path_nested () =
  let result = Hermes_ppx.build_module_path ~loc ["App"; "Bsky"; "Graph"] in
  check string "nested module" "App.Bsky.Graph"
    (Ppxlib.Longident.name result.txt)

let test_build_call_expr () =
  let result = Hermes_ppx.build_call_expr ~loc "app.bsky.graph.getProfile" in
  let expected_str = "App.Bsky.Graph.GetProfile.call" in
  check string "call expr" expected_str
    (Ppxlib.Pprintast.string_of_expression result)

let expand_xrpc code =
  let lexbuf = Lexing.from_string code in
  let structure = Ppxlib.Parse.implementation lexbuf in
  let transformed = Ppxlib.Driver.map_structure structure in
  match transformed with
  | [{Ppxlib.pstr_desc= Ppxlib.Pstr_eval (expr, _); _}] ->
      expr
  | [{Ppxlib.pstr_desc= Ppxlib.Pstr_value (_, [{pvb_expr; _}]); _}] ->
      pvb_expr
  | _ ->
      failwith "unexpected structure after expansion"

let test_expand_get_nsid () =
  let actual = expand_xrpc {|[%xrpc get "app.bsky.graph.getRelationships"]|} in
  let expected_str = "App.Bsky.Graph.GetRelationships.call" in
  check string "get expansion" expected_str
    (Ppxlib.Pprintast.string_of_expression actual)

let test_expand_post_nsid () =
  let actual =
    expand_xrpc {|[%xrpc post "com.atproto.server.createSession"]|}
  in
  let expected_str = "Com.Atproto.Server.CreateSession.call" in
  check string "post expansion" expected_str
    (Ppxlib.Pprintast.string_of_expression actual)

let test_expand_nsid_only () =
  (* [%xrpc "nsid"] defaults to get *)
  let actual = expand_xrpc {|[%xrpc "app.bsky.actor.getProfile"]|} in
  let expected_str = "App.Bsky.Actor.GetProfile.call" in
  check string "nsid only expansion" expected_str
    (Ppxlib.Pprintast.string_of_expression actual)

let unit_tests =
  [ ("nsid_to_module_path simple", `Quick, test_nsid_to_module_path_simple)
  ; ( "nsid_to_module_path camelCase"
    , `Quick
    , test_nsid_to_module_path_camel_case )
  ; ("nsid_to_module_path single", `Quick, test_nsid_to_module_path_single)
  ; ("build_module_path single", `Quick, test_build_module_path_single)
  ; ("build_module_path nested", `Quick, test_build_module_path_nested)
  ; ("build_call_expr", `Quick, test_build_call_expr) ]

let expansion_tests =
  [ ("expand get nsid", `Quick, test_expand_get_nsid)
  ; ("expand post nsid", `Quick, test_expand_post_nsid)
  ; ("expand nsid only", `Quick, test_expand_nsid_only) ]

let () = run "hermes_ppx" [("unit", unit_tests); ("expansion", expansion_tests)]
