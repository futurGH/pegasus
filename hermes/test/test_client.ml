open Alcotest
open Lwt.Syntax
open Test_support

let run_lwt f = Lwt_main.run (f ())

(* helpers *)
let test_string = testable Fmt.string String.equal

let test_bytes =
  testable
    (Fmt.of_to_string (fun b -> String.sub (Bytes.to_string b) 0 10))
    Bytes.equal

(** query tests *)

let test_query_success () =
  run_lwt
  @@ fun () ->
  let response =
    Mock_http.json_response
      (`Assoc
         [("did", `String "did:plc:123"); ("handle", `String "test.bsky.social")]
      )
  in
  let* result, requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        C.query client "com.atproto.identity.resolveHandle"
          (`Assoc [("handle", `String "test.bsky.social")])
          (fun json ->
            let open Yojson.Safe.Util in
            Ok (json |> member "did" |> to_string) ) )
  in
  check test_string "result" "did:plc:123" result ;
  check int "request count" 1 (List.length requests) ;
  let req = List.hd requests in
  Test_utils.assert_request_path "/xrpc/com.atproto.identity.resolveHandle" req ;
  Test_utils.assert_request_method `GET req ;
  Test_utils.assert_request_query_param "handle" "test.bsky.social" req ;
  Lwt.return_unit

let test_query_with_multiple_params () =
  run_lwt
  @@ fun () ->
  let response = Mock_http.json_response (`Assoc [("followers", `List [])]) in
  let* _, requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        C.query client "app.bsky.graph.getFollowers"
          (`Assoc
             [ ("actor", `String "did:plc:123")
             ; ("limit", `Int 50)
             ; ("cursor", `String "abc123") ] )
          (fun _ -> Ok ()) )
  in
  let req = List.hd requests in
  Test_utils.assert_request_query_param "actor" "did:plc:123" req ;
  Test_utils.assert_request_query_param "limit" "50" req ;
  Test_utils.assert_request_query_param "cursor" "abc123" req ;
  Lwt.return_unit

let test_query_error_response () =
  run_lwt
  @@ fun () ->
  let response =
    Mock_http.error_response ~status:`Bad_request ~error:"InvalidHandle"
      ~message:"Handle not found" ()
  in
  let* () =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        Lwt.catch
          (fun () ->
            let* _ =
              C.query client "com.atproto.identity.resolveHandle"
                (`Assoc [("handle", `String "invalid")])
                (fun _ -> Ok ())
            in
            fail "should have raised Xrpc_error" )
          (function
            | Hermes.Xrpc_error {status; error; message} ->
                check int "status" 400 status ;
                check test_string "error" "InvalidHandle" error ;
                check (option test_string) "message" (Some "Handle not found")
                  message ;
                Lwt.return_unit
            | e ->
                Lwt.reraise e ) )
    |> Lwt.map fst
  in
  Lwt.return_unit

let test_query_empty_response () =
  run_lwt
  @@ fun () ->
  let response = Mock_http.empty_response () in
  let* result, _ =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        C.query client "some.endpoint" (`Assoc []) (fun _ -> Ok "empty") )
  in
  check test_string "result" "empty" result ;
  Lwt.return_unit

let test_query_bytes () =
  run_lwt
  @@ fun () ->
  let response =
    Mock_http.bytes_response ~content_type:"image/jpeg" "fake-image-data"
  in
  let* (data, content_type), requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        C.query_bytes client "com.atproto.sync.getBlob"
          (`Assoc [("did", `String "did:plc:123"); ("cid", `String "bafyabc")]) )
  in
  check test_bytes "data" (Bytes.of_string "fake-image-data") data ;
  check test_string "content_type" "image/jpeg" content_type ;
  let req = List.hd requests in
  Test_utils.assert_request_has_header "accept" "*/*" req ;
  Lwt.return_unit

(** procedure tests *)

let test_procedure_success () =
  run_lwt
  @@ fun () ->
  let response =
    Mock_http.json_response
      (`Assoc [("uri", `String "at://did:plc:123/app.bsky.feed.post/abc")])
  in
  let* result, requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        C.procedure client "com.atproto.repo.createRecord" (`Assoc [])
          (Some
             (`Assoc
                [ ("repo", `String "did:plc:123")
                ; ("collection", `String "app.bsky.feed.post")
                ; ( "record"
                  , `Assoc [("text", `String "This post was sent from PDSls")]
                  ) ] ) )
          (fun json ->
            let open Yojson.Safe.Util in
            Ok (json |> member "uri" |> to_string) ) )
  in
  check test_string "uri" "at://did:plc:123/app.bsky.feed.post/abc" result ;
  let req = List.hd requests in
  Test_utils.assert_request_method `POST req ;
  Test_utils.assert_request_path "/xrpc/com.atproto.repo.createRecord" req ;
  Test_utils.assert_request_has_header "content-type" "application/json" req ;
  Test_utils.assert_request_body_contains "This post was sent from PDSls" req ;
  Lwt.return_unit

let test_procedure_no_input () =
  run_lwt
  @@ fun () ->
  let response = Mock_http.empty_response () in
  let* _, requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        C.procedure client "com.atproto.server.deleteSession" (`Assoc []) None
          (fun _ -> Ok () ) )
  in
  let req = List.hd requests in
  Test_utils.assert_request_method `POST req ;
  check (option test_string) "body" (Some "") req.body ;
  Lwt.return_unit

let test_procedure_bytes () =
  run_lwt
  @@ fun () ->
  let response = Mock_http.empty_response () in
  let* result, requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        C.procedure_bytes client "com.atproto.repo.importRepo" (`Assoc [])
          (Some (Bytes.of_string "fake-car-data"))
          ~content_type:"application/vnd.ipld.car" )
  in
  check (option (pair test_bytes test_string)) "result" None result ;
  let req = List.hd requests in
  Test_utils.assert_request_has_header "content-type" "application/vnd.ipld.car"
    req ;
  Test_utils.assert_request_has_header "accept" "*/*" req ;
  check (option test_string) "body" (Some "fake-car-data") req.body ;
  Lwt.return_unit

let test_procedure_blob () =
  run_lwt
  @@ fun () ->
  let response =
    Mock_http.json_response
      (`Assoc
         [ ( "blob"
           , `Assoc
               [ ("$type", `String "blob")
               ; ("ref", `Assoc [("$link", `String "bafyabc")])
               ; ("mimeType", `String "image/jpeg")
               ; ("size", `Int 1234) ] ) ] )
  in
  let* result, requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        C.procedure_blob client "com.atproto.repo.uploadBlob" (`Assoc [])
          (Bytes.of_string "fake-image-bytes") ~content_type:"image/jpeg"
          (fun json ->
            let open Yojson.Safe.Util in
            Ok (json |> member "blob" |> member "mimeType" |> to_string) ) )
  in
  check test_string "mimeType" "image/jpeg" result ;
  let req = List.hd requests in
  Test_utils.assert_request_has_header "content-type" "image/jpeg" req ;
  check (option test_string) "body" (Some "fake-image-bytes") req.body ;
  Lwt.return_unit

(** authentication tests *)

let test_auth_header_added () =
  run_lwt
  @@ fun () ->
  let response = Mock_http.json_response (`Assoc []) in
  let* _, requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        let session = Test_utils.make_test_session () in
        C.set_session client session ;
        C.query client "some.endpoint" (`Assoc []) (fun _ -> Ok ()) )
  in
  let req = List.hd requests in
  Test_utils.assert_request_has_auth_header req ;
  Lwt.return_unit

let test_session_can_be_cleared () =
  run_lwt
  @@ fun () ->
  let response = Mock_http.json_response (`Assoc []) in
  let* _, requests =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        let session = Test_utils.make_test_session () in
        C.set_session client session ;
        C.clear_session client ;
        C.query client "some.endpoint" (`Assoc []) (fun _ -> Ok ()) )
  in
  let req = List.hd requests in
  let has_auth = Cohttp.Header.get req.headers "authorization" in
  check (option test_string) "no auth header" None has_auth ;
  Lwt.return_unit

(** error handling tests *)

let test_401_unauthorized () =
  run_lwt
  @@ fun () ->
  let response =
    Mock_http.error_response ~status:`Unauthorized ~error:"AuthRequired"
      ~message:"Authentication required" ()
  in
  let* () =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        Lwt.catch
          (fun () ->
            let* _ =
              C.query client "some.protected.endpoint" (`Assoc []) (fun _ ->
                  Ok () )
            in
            fail "should have raised" )
          (function
            | Hermes.Xrpc_error {status= 401; error= "AuthRequired"; _} ->
                Lwt.return_unit
            | e ->
                Lwt.reraise e ) )
    |> Lwt.map fst
  in
  Lwt.return_unit

let test_500_server_error () =
  run_lwt
  @@ fun () ->
  let response =
    Mock_http.error_response ~status:`Internal_server_error
      ~error:"InternalServerError" ()
  in
  let* () =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        Lwt.catch
          (fun () ->
            let* _ =
              C.query client "some.endpoint" (`Assoc []) (fun _ -> Ok ())
            in
            fail "Should have raised" )
          (function
            | Hermes.Xrpc_error {status= 500; _} ->
                Lwt.return_unit
            | e ->
                Lwt.reraise e ) )
    |> Lwt.map fst
  in
  Lwt.return_unit

let test_malformed_error_response () =
  run_lwt
  @@ fun () ->
  let response =
    { Mock_http.status= `Bad_request
    ; headers= [("content-type", "application/json")]
    ; body= "not valid json" }
  in
  let* () =
    Test_utils.with_mock_responses [response] (fun (module C) client ->
        Lwt.catch
          (fun () ->
            let* _ =
              C.query client "some.endpoint" (`Assoc []) (fun _ -> Ok ())
            in
            fail "should have raised" )
          (function
            | Hermes.Xrpc_error {status= 400; error= "UnknownError"; _} ->
                Lwt.return_unit
            | e ->
                Lwt.reraise e ) )
    |> Lwt.map fst
  in
  Lwt.return_unit

(** client creation tests *)

let test_make_client () =
  let client = Hermes.make_client ~service:"https://api.bsky.app" () in
  let service = Hermes.get_service client in
  check (option test_string) "host" (Some "api.bsky.app") (Uri.host service)

let test_client_service_urls () =
  let urls =
    [ "https://bsky.social"
    ; "https://api.bsky.app"
    ; "http://localhost:3000"
    ; "https://pds.example.com:8080" ]
  in
  List.iter
    (fun url ->
      let client = Hermes.make_client ~service:url () in
      let service = Hermes.get_service client in
      check bool "service set" true (String.length (Uri.to_string service) > 0) )
    urls

let test_get_session_unauthenticated () =
  let client = Hermes.make_client ~service:"https://example.com" () in
  check (option reject) "no session" None (Hermes.get_session client)

(** tests *)

let query_tests =
  [ ("query success", `Quick, test_query_success)
  ; ("query with multiple params", `Quick, test_query_with_multiple_params)
  ; ("query error response", `Quick, test_query_error_response)
  ; ("query empty response", `Quick, test_query_empty_response)
  ; ("query bytes", `Quick, test_query_bytes) ]

let procedure_tests =
  [ ("procedure success", `Quick, test_procedure_success)
  ; ("procedure no input", `Quick, test_procedure_no_input)
  ; ("procedure bytes", `Quick, test_procedure_bytes)
  ; ("procedure blob", `Quick, test_procedure_blob) ]

let auth_tests =
  [ ("auth header added", `Quick, test_auth_header_added)
  ; ("session can be cleared", `Quick, test_session_can_be_cleared) ]

let error_tests =
  [ ("401 unauthorized", `Quick, test_401_unauthorized)
  ; ("500 server error", `Quick, test_500_server_error)
  ; ("malformed error response", `Quick, test_malformed_error_response) ]

let creation_tests =
  [ ("make_client", `Quick, test_make_client)
  ; ("service URLs", `Quick, test_client_service_urls)
  ; ("get_session unauthenticated", `Quick, test_get_session_unauthenticated) ]

let () =
  run "Client"
    [ ("query", query_tests)
    ; ("procedure", procedure_tests)
    ; ("auth", auth_tests)
    ; ("errors", error_tests)
    ; ("creation", creation_tests) ]
