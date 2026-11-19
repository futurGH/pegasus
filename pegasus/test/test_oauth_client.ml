open Alcotest

let client_id = "https://parfait.example.com/oauth-client-metadata.json"

let accepts ?(application_type = "web") uri =
  match
    Pegasus.Oauth.Client.validate_redirect_uri ~client_id ~application_type uri
  with
  | () ->
      true
  | exception Failure _ ->
      false

let test_web_redirects () =
  check bool "https" true (accepts "https://parfait.example.com/callback") ;
  check bool "http loopback" true (accepts "http://127.0.0.1:8080/callback") ;
  check bool "https localhost" false (accepts "https://localhost/callback") ;
  check bool "plain http" false (accepts "http://parfait.example.com/callback") ;
  check bool "private-use scheme on a web client" false
    (accepts "com.example.parfait:/callback")

let test_native_redirects () =
  let native = accepts ~application_type:"native" in
  check bool "client_id's hostname reversed" true
    (native "com.example.parfait:/oauth/callback") ;
  check bool "any case" true (native "COM.Example.Parfait:/oauth/callback") ;
  check bool "https" true (native "https://parfait.example.com/callback") ;
  check bool "http loopback" true (native "http://[::1]/callback") ;
  check bool "another domain's scheme" false (native "com.other.app:/callback") ;
  check bool "parent domain's scheme" false (native "com.example:/callback") ;
  check bool "scheme without a dot" false (native "parfait:/callback") ;
  check bool "two slashes" false (native "com.example.parfait://callback") ;
  check bool "three slashes" false (native "com.example.parfait:///callback")

let () =
  run "oauth client"
    [ ( "redirect uris"
      , [ ("web", `Quick, test_web_redirects)
        ; ("native", `Quick, test_native_redirects) ] ) ]
