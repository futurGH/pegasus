open Alcotest
open Pegasus.Oauth.Scopes

let test_string = testable Fmt.string String.equal

let test_nsid_authority () =
  check test_string "three segments" "com.example"
    (Pegasus.Util.nsid_authority "com.example.foo") ;
  check test_string "four segments" "com.example.app"
    (Pegasus.Util.nsid_authority "com.example.app.auth") ;
  check test_string "two segments" "com"
    (Pegasus.Util.nsid_authority "com.example")

let test_is_parent_authority () =
  check bool "same authority" true
    (is_parent_authority_of ~include_nsid:"com.example.app.auth"
       ~permission_nsid:"com.example.app.calendar" ) ;
  check bool "child authority" true
    (is_parent_authority_of ~include_nsid:"com.example.app.auth"
       ~permission_nsid:"com.example.app.sub.thing" ) ;
  check bool "different authority" false
    (is_parent_authority_of ~include_nsid:"com.example.app.auth"
       ~permission_nsid:"org.other.thing" ) ;
  check bool "partial match not ok" false
    (is_parent_authority_of ~include_nsid:"com.example.app.auth"
       ~permission_nsid:"com.example.different" )

(* test parse_scope for include scopes *)
let test_parse_include_scope () =
  (* valid include scope with aud *)
  ( match
      parse_scope "include:com.example.app.auth?aud=did:web:api.example.com"
    with
  | Some (Include {nsid; aud}) ->
      check test_string "nsid" "com.example.app.auth" nsid ;
      check (option test_string) "aud" (Some "did:web:api.example.com") aud
  | _ ->
      fail "expected Include scope" ) ;
  (* valid include scope without aud *)
  ( match parse_scope "include:com.example.app.perms" with
  | Some (Include {nsid; aud}) ->
      check test_string "nsid" "com.example.app.perms" nsid ;
      check (option test_string) "aud" None aud
  | _ ->
      fail "expected Include scope" ) ;
  (* bad nsid *)
  ( match parse_scope "include:invalid" with
  | None ->
      ()
  | Some _ ->
      fail "expected None for invalid nsid" ) ;
  (* bad aud *)
  match parse_scope "include:com.example.foo?aud=notadid" with
  | None ->
      ()
  | Some _ ->
      fail "expected None for invalid aud"

let test_permission_to_scope () =
  let open Pegasus.Lexicon_resolver in
  (* rpc permission with explicit aud *)
  let rpc_perm =
    { resource= "rpc"
    ; lxm= Some ["com.example.foo"; "com.example.bar"]
    ; aud= Some "did:web:api.example.com"
    ; inherit_aud= None
    ; collection= None
    ; action= None
    ; accept= None }
  in
  ( match permission_to_scope ~include_aud:None rpc_perm with
  | Some scopes ->
      check int "two rpc scopes" 2 (List.length scopes) ;
      (* check that first scope starts with expected pattern *)
      check bool "first scope valid" true
        (String.starts_with ~prefix:"rpc:com.example.foo?aud="
           (List.nth scopes 0) )
  | None ->
      fail "expected Some scopes" ) ;
  (* rpc permission with inheritAud *)
  let rpc_inherit =
    { resource= "rpc"
    ; lxm= Some ["com.example.baz"]
    ; aud= None
    ; inherit_aud= Some true
    ; collection= None
    ; action= None
    ; accept= None }
  in
  ( match
      permission_to_scope ~include_aud:(Some "did:plc:inherited") rpc_inherit
    with
  | Some scopes ->
      check int "inherited aud single scope" 1 (List.length scopes) ;
      check bool "inherited aud scope valid" true
        (String.starts_with ~prefix:"rpc:com.example.baz?aud="
           (List.nth scopes 0) )
  | None ->
      fail "expected scopes with inherited aud" ) ;
  (* repo permission included *)
  let repo_perm =
    { resource= "repo"
    ; lxm= None
    ; aud= None
    ; inherit_aud= None
    ; collection= Some ["com.example.data"]
    ; action= Some ["create"; "update"]
    ; accept= None }
  in
  ( match permission_to_scope ~include_aud:None repo_perm with
  | Some [scope] ->
      check bool "repo scope" true
        (String.starts_with ~prefix:"repo:com.example.data" scope)
  | _ ->
      fail "expected single repo scope" ) ;
  (* account permission filtered out *)
  let account_perm =
    { resource= "account"
    ; lxm= None
    ; aud= None
    ; inherit_aud= None
    ; collection= None
    ; action= None
    ; accept= None }
  in
  match permission_to_scope ~include_aud:None account_perm with
  | None ->
      ()
  | Some _ ->
      fail "account should be filtered"

let test_expand_include_scope_authority () =
  let open Pegasus.Lexicon_resolver in
  let inc : include_scope =
    {nsid= "com.example.app.auth"; aud= Some "did:web:api.example.com"}
  in
  let ps =
    { title= Some "Test"
    ; title_lang= None
    ; detail= None
    ; detail_lang= None
    ; permissions=
        [ (* valid under com.example.app authority *)
          { resource= "rpc"
          ; lxm= Some ["com.example.app.login"]
          ; aud= None
          ; inherit_aud= Some true
          ; collection= None
          ; action= None
          ; accept= None }
        ; (* invalid, different authority *)
          { resource= "rpc"
          ; lxm= Some ["org.other.thing"]
          ; aud= None
          ; inherit_aud= Some true
          ; collection= None
          ; action= None
          ; accept= None } ] }
  in
  let expanded = expand_include_scope inc ps in
  check int "only valid permission expanded" 1 (List.length expanded) ;
  (* check that we have at least one scope starting with rpc: *)
  check bool "has rpc scope" true
    ( List.length expanded > 0
    && String.starts_with ~prefix:"rpc:" (List.hd expanded) )

let () =
  run "scopes"
    [ ( "authority"
      , [ ("nsid_authority", `Quick, test_nsid_authority)
        ; ("is_parent_authority_of", `Quick, test_is_parent_authority) ] )
    ; ("include", [("parse_include_scope", `Quick, test_parse_include_scope)])
    ; ( "expansion"
      , [ ("permission_to_scope", `Quick, test_permission_to_scope)
        ; ( "expand_include_scope_authority"
          , `Quick
          , test_expand_include_scope_authority ) ] ) ]
