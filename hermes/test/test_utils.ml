(** test utilities *)

open Lwt.Syntax

(* run a test with a mock HTTP client using queued responses *)
let with_mock_responses responses f =
  let queue = Mock_http.Queue.create responses in
  let handler_ref = ref (Mock_http.Queue.handler queue) in
  let module MockHttp = Mock_http.Make (struct
    let handler = handler_ref
  end) in
  let module MockClient = Hermes.Client.Make (MockHttp) in
  let client = MockClient.make ~service:"https://test.example.com" () in
  let* result = f (module MockClient : Hermes.Client.S) client in
  Lwt.return (result, Mock_http.Queue.get_requests queue)

(* run a test with a mock HTTP client using pattern matching *)
let with_mock_patterns ?default rules f =
  let pattern = Mock_http.Pattern.create ?default rules in
  let handler_ref = ref (Mock_http.Pattern.handler pattern) in
  let module MockHttp = Mock_http.Make (struct
    let handler = handler_ref
  end) in
  let module MockClient = Hermes.Client.Make (MockHttp) in
  let client = MockClient.make ~service:"https://test.example.com" () in
  let* result = f (module MockClient : Hermes.Client.S) client in
  Lwt.return (result, Mock_http.Pattern.get_requests pattern)

(* create a valid JWT for testing *)
let make_test_jwt ?(exp_offset = 3600) ?(sub = "did:plc:test") () =
  let now = int_of_float (Unix.time ()) in
  let exp = now + exp_offset in
  let header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" in
  let payload_json =
    Printf.sprintf {|{"sub":"%s","iat":%d,"exp":%d}|} sub now exp
  in
  let payload =
    Base64.encode_string ~alphabet:Base64.uri_safe_alphabet ~pad:false
      payload_json
  in
  header ^ "." ^ payload ^ ".fake_signature"

(* create a test session *)
let make_test_session ?(exp_offset = 3600) () : Hermes.session =
  let jwt = make_test_jwt ~exp_offset () in
  { access_jwt= jwt
  ; refresh_jwt= make_test_jwt ~exp_offset:86400 ()
  ; did= "did:plc:testuser123"
  ; handle= "test.bsky.social"
  ; pds_uri= Some "https://pds.example.com"
  ; email= Some "test@example.com"
  ; email_confirmed= Some true
  ; email_auth_factor= Some false
  ; active= Some true
  ; status= None }

(* create a session response JSON *)
let session_response_json ?(exp_offset = 3600) () =
  let session = make_test_session ~exp_offset () in
  `Assoc
    [ ("accessJwt", `String session.access_jwt)
    ; ("refreshJwt", `String session.refresh_jwt)
    ; ("did", `String session.did)
    ; ("handle", `String session.handle) ]

(** assert helpers for requests *)

let assert_request_path expected_path (req : Mock_http.request) =
  let actual = Uri.path req.uri in
  if actual <> expected_path then
    failwith (Printf.sprintf "expected path %s but got %s" expected_path actual)

let assert_request_method expected_meth (req : Mock_http.request) =
  if req.meth <> expected_meth then
    let expected_str =
      match expected_meth with `GET -> "GET" | `POST -> "POST"
    in
    let actual_str = match req.meth with `GET -> "GET" | `POST -> "POST" in
    failwith
      (Printf.sprintf "expected method %s but got %s" expected_str actual_str)

let assert_request_has_header name value (req : Mock_http.request) =
  match Cohttp.Header.get req.headers name with
  | Some v when v = value ->
      ()
  | Some v ->
      failwith (Printf.sprintf "header %s: expected %s but got %s" name value v)
  | None ->
      failwith (Printf.sprintf "header %s not found" name)

let assert_request_has_auth_header (req : Mock_http.request) =
  match Cohttp.Header.get req.headers "authorization" with
  | Some v when String.length v > 7 && String.sub v 0 7 = "Bearer " ->
      ()
  | Some v ->
      failwith (Printf.sprintf "invalid auth header: %s" v)
  | None ->
      failwith "authorization header not found"

let assert_request_query_param name expected_value (req : Mock_http.request) =
  let query = Uri.query req.uri in
  match List.assoc_opt name query with
  | Some [v] when v = expected_value ->
      ()
  | Some [v] ->
      failwith
        (Printf.sprintf "query param %s: expected %s but got %s" name
           expected_value v )
  | Some vs ->
      failwith
        (Printf.sprintf "query param %s has multiple values: %s" name
           (String.concat ", " vs) )
  | None ->
      failwith (Printf.sprintf "query param %s not found" name)

let assert_request_body_contains substring (req : Mock_http.request) =
  match req.body with
  | Some body when String.length body > 0 ->
      if
        not
          ( String.length substring <= String.length body
          &&
          let rec check i =
            if i > String.length body - String.length substring then false
            else if String.sub body i (String.length substring) = substring then
              true
            else check (i + 1)
          in
          check 0 )
      then failwith (Printf.sprintf "body does not contain '%s'" substring)
  | _ ->
      failwith "expected request body but none found"

(* run a test with a mock credential manager *)
let with_mock_credential_manager responses f =
  let queue = Mock_http.Queue.create responses in
  let handler_ref = ref (Mock_http.Queue.handler queue) in
  let module MockHttp = Mock_http.Make (struct
    let handler = handler_ref
  end) in
  let module MockClient = Hermes.Client.Make (MockHttp) in
  let module MockCredManager = Hermes.Credential_manager.Make (MockClient) in
  let manager = MockCredManager.make ~service:"https://test.example.com" () in
  let* result =
    f
      (module MockCredManager : Hermes.Credential_manager.S)
      (module MockClient : Hermes.Client.S)
      manager
  in
  Lwt.return (result, Mock_http.Queue.get_requests queue)
