open Alcotest

let params =
  [ ("iss", "https://pegasuspds.com")
  ; ("state", "state with spaces")
  ; ("code", "cod-123") ]

let test_fragment_response () =
  let actual =
    Pegasus.Api.Oauth_.Authorize.oauth_redirect_url
      "https://bsky.app/auth/web/callback" (Some "fragment") params
  in
  check string "fragment parameters remain independently parseable"
    "https://bsky.app/auth/web/callback#iss=https%3A%2F%2Fpegasuspds.com&state=state%20with%20spaces&code=cod-123"
    actual

let test_fragment_replaces_registered_fragment () =
  let actual =
    Pegasus.Api.Oauth_.Authorize.oauth_redirect_url
      "https://client.example/callback#registered-fragment" (Some "fragment")
      params
  in
  check string "OAuth response replaces the registered fragment"
    "https://client.example/callback#iss=https%3A%2F%2Fpegasuspds.com&state=state%20with%20spaces&code=cod-123"
    actual

let test_query_response () =
  let actual =
    Pegasus.Api.Oauth_.Authorize.oauth_redirect_url
      "https://client.example/callback?existing=1#registered-fragment" None
      params
  in
  check string "query response preserves query and registered fragment"
    "https://client.example/callback?existing=1&iss=https%3A%2F%2Fpegasuspds.com&state=state%20with%20spaces&code=cod-123#registered-fragment"
    actual

let () =
  run "oauth redirect"
    [ ( "response modes"
      , [ ("fragment", `Quick, test_fragment_response)
        ; ( "fragment replaces registered fragment"
          , `Quick
          , test_fragment_replaces_registered_fragment )
        ; ("query", `Quick, test_query_response) ] ) ]
