(** tests for Hermes.Credential_manager *)

open Alcotest
open Lwt.Syntax
open Test_support

let run_lwt f = Lwt_main.run (f ())

(* helpers *)
let test_string = testable Fmt.string String.equal

(** login tests *)

let test_login_success () =
  run_lwt
  @@ fun () ->
  let session = Test_utils.make_test_session () in
  let response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String session.access_jwt)
         ; ("refreshJwt", `String session.refresh_jwt)
         ; ("did", `String session.did)
         ; ("handle", `String session.handle) ] )
  in
  let* client, requests =
    Test_utils.with_mock_credential_manager [response]
      (fun (module CM) (module C) manager ->
        let* client =
          CM.login manager ~identifier:"test@example.com" ~password:"secret" ()
        in
        Lwt.return client )
  in
  check int "request count" 1 (List.length requests) ;
  let req = List.hd requests in
  Test_utils.assert_request_path "/xrpc/com.atproto.server.createSession" req ;
  Test_utils.assert_request_method `POST req ;
  Test_utils.assert_request_body_contains "test@example.com" req ;
  Test_utils.assert_request_body_contains "secret" req ;
  check bool "client has session" true (Hermes.get_session client <> None) ;
  Lwt.return_unit

let test_login_error () =
  run_lwt
  @@ fun () ->
  let response =
    Mock_http.error_response ~status:`Unauthorized
      ~error:"AuthenticationRequired" ~message:"Invalid credentials" ()
  in
  let* () =
    Test_utils.with_mock_credential_manager [response]
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        let _ = C.make in
        (* suppress unused warning *)
        Lwt.catch
          (fun () ->
            let* _ =
              CM.login manager ~identifier:"test@example.com" ~password:"wrong"
                ()
            in
            fail "should have raised Xrpc_error" )
          (function
            | Hermes.Xrpc_error {status; error; _} ->
                check int "status" 401 status ;
                check test_string "error" "AuthenticationRequired" error ;
                Lwt.return_unit
            | e ->
                Lwt.reraise e ) )
    |> Lwt.map fst
  in
  Lwt.return_unit

let test_login_with_auth_factor () =
  run_lwt
  @@ fun () ->
  let session = Test_utils.make_test_session () in
  let response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String session.access_jwt)
         ; ("refreshJwt", `String session.refresh_jwt)
         ; ("did", `String session.did)
         ; ("handle", `String session.handle) ] )
  in
  let* _, requests =
    Test_utils.with_mock_credential_manager [response]
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        let* _ =
          CM.login manager ~identifier:"test@example.com" ~password:"secret"
            ~auth_factor_token:"123456" ()
        in
        Lwt.return () )
  in
  let req = List.hd requests in
  Test_utils.assert_request_body_contains "123456" req ;
  Lwt.return_unit

(* resume session tests *)

let test_resume_session () =
  run_lwt
  @@ fun () ->
  let session = Test_utils.make_test_session () in
  let* client, _requests =
    Test_utils.with_mock_credential_manager []
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        let* client = CM.resume manager ~session () in
        Lwt.return client )
  in
  let client_session = Hermes.get_session client in
  check bool "client has session" true (client_session <> None) ;
  let s = Option.get client_session in
  check test_string "did matches" session.did s.did ;
  check test_string "handle matches" session.handle s.handle ;
  Lwt.return_unit

(** session callback tests *)

let test_session_update_callback () =
  run_lwt
  @@ fun () ->
  let session = Test_utils.make_test_session () in
  let response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String session.access_jwt)
         ; ("refreshJwt", `String session.refresh_jwt)
         ; ("did", `String session.did)
         ; ("handle", `String session.handle) ] )
  in
  let callback_called = ref false in
  let received_session = ref None in
  let* _, _ =
    Test_utils.with_mock_credential_manager [response]
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        CM.on_session_update manager (fun s ->
            callback_called := true ;
            received_session := Some s ;
            Lwt.return_unit ) ;
        let* _ =
          CM.login manager ~identifier:"test@example.com" ~password:"secret" ()
        in
        Lwt.return () )
  in
  check bool "callback was called" true !callback_called ;
  check bool "session was received" true (!received_session <> None) ;
  let s = Option.get !received_session in
  check test_string "received did" session.did s.did ;
  Lwt.return_unit

let test_session_expired_callback () =
  run_lwt
  @@ fun () ->
  (* first log in, then log out with server error; should still call expired callback *)
  let session = Test_utils.make_test_session () in
  let login_response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String session.access_jwt)
         ; ("refreshJwt", `String session.refresh_jwt)
         ; ("did", `String session.did)
         ; ("handle", `String session.handle) ] )
  in
  (* log out endpoint returns error but should still clear session *)
  let logout_response =
    Mock_http.error_response ~status:`Internal_server_error
      ~error:"InternalServerError" ()
  in
  let expired_called = ref false in
  let* _, _ =
    Test_utils.with_mock_credential_manager [login_response; logout_response]
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        CM.on_session_expired manager (fun () ->
            expired_called := true ;
            Lwt.return_unit ) ;
        let* _ =
          CM.login manager ~identifier:"test@example.com" ~password:"secret" ()
        in
        let* () = CM.logout manager in
        Lwt.return () )
  in
  check bool "expired callback was called" true !expired_called ;
  Lwt.return_unit

(** log out tests *)

let test_logout_success () =
  run_lwt
  @@ fun () ->
  let session = Test_utils.make_test_session () in
  let login_response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String session.access_jwt)
         ; ("refreshJwt", `String session.refresh_jwt)
         ; ("did", `String session.did)
         ; ("handle", `String session.handle) ] )
  in
  let logout_response = Mock_http.empty_response () in
  let* manager_session, requests =
    Test_utils.with_mock_credential_manager [login_response; logout_response]
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        let* _ =
          CM.login manager ~identifier:"test@example.com" ~password:"secret" ()
        in
        let* () = CM.logout manager in
        Lwt.return (CM.get_session manager) )
  in
  check (option reject) "manager session cleared" None manager_session ;
  check int "request count" 2 (List.length requests) ;
  let logout_req = List.nth requests 1 in
  Test_utils.assert_request_path "/xrpc/com.atproto.server.deleteSession"
    logout_req ;
  Test_utils.assert_request_method `POST logout_req ;
  Lwt.return_unit

let test_logout_no_session () =
  run_lwt
  @@ fun () ->
  let* _, requests =
    Test_utils.with_mock_credential_manager []
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        let* () = CM.logout manager in
        Lwt.return () )
  in
  check int "no requests made" 0 (List.length requests) ;
  Lwt.return_unit

(* ===== Get Session Tests ===== *)

let test_get_session_before_login () =
  run_lwt
  @@ fun () ->
  let* session, _ =
    Test_utils.with_mock_credential_manager []
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        Lwt.return (CM.get_session manager) )
  in
  check (option reject) "no session" None session ;
  Lwt.return_unit

let test_get_session_after_login () =
  run_lwt
  @@ fun () ->
  let session = Test_utils.make_test_session () in
  let response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String session.access_jwt)
         ; ("refreshJwt", `String session.refresh_jwt)
         ; ("did", `String session.did)
         ; ("handle", `String session.handle) ] )
  in
  let* manager_session, _ =
    Test_utils.with_mock_credential_manager [response]
      (fun (module CM) (module C : Hermes.Client.S) manager ->
        let* _ =
          CM.login manager ~identifier:"test@example.com" ~password:"secret" ()
        in
        Lwt.return (CM.get_session manager) )
  in
  check bool "has session" true (manager_session <> None) ;
  let s = Option.get manager_session in
  check test_string "did matches" session.did s.did ;
  Lwt.return_unit

(** token refresh tests *)

let test_automatic_token_refresh () =
  run_lwt
  @@ fun () ->
  (* create session with token expiring soon *)
  let expired_session = Test_utils.make_test_session ~exp_offset:60 () in
  let new_session = Test_utils.make_test_session ~exp_offset:3600 () in
  let login_response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String expired_session.access_jwt)
         ; ("refreshJwt", `String expired_session.refresh_jwt)
         ; ("did", `String expired_session.did)
         ; ("handle", `String expired_session.handle) ] )
  in
  let refresh_response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String new_session.access_jwt)
         ; ("refreshJwt", `String new_session.refresh_jwt)
         ; ("did", `String new_session.did)
         ; ("handle", `String new_session.handle) ] )
  in
  (* API call response after refresh *)
  let api_response =
    Mock_http.json_response (`Assoc [("data", `String "success")])
  in
  let* result, requests =
    Test_utils.with_mock_credential_manager
      [login_response; refresh_response; api_response]
      (fun (module CM) (module C) manager ->
        let* client =
          CM.login manager ~identifier:"test@example.com" ~password:"secret" ()
        in
        (* making an API call should trigger token refresh *)
        let* result =
          C.query client "test.endpoint" (`Assoc []) (fun json ->
              let open Yojson.Safe.Util in
              Ok (json |> member "data" |> to_string) )
        in
        Lwt.return result )
  in
  check test_string "api result" "success" result ;
  check int "request count" 3 (List.length requests) ;
  (* second request should be refresh *)
  let refresh_req = List.nth requests 1 in
  Test_utils.assert_request_path "/xrpc/com.atproto.server.refreshSession"
    refresh_req ;
  Lwt.return_unit

let test_refresh_failure_clears_session () =
  run_lwt
  @@ fun () ->
  (* create session with expired token *)
  let expired_session = Test_utils.make_test_session ~exp_offset:60 () in
  let login_response =
    Mock_http.json_response
      (`Assoc
         [ ("accessJwt", `String expired_session.access_jwt)
         ; ("refreshJwt", `String expired_session.refresh_jwt)
         ; ("did", `String expired_session.did)
         ; ("handle", `String expired_session.handle) ] )
  in
  (* refresh fails with ExpiredToken *)
  let refresh_response =
    Mock_http.error_response ~status:`Bad_request ~error:"ExpiredToken"
      ~message:"Refresh token expired" ()
  in
  let expired_called = ref false in
  let* () =
    Test_utils.with_mock_credential_manager [login_response; refresh_response]
      (fun (module CM) (module C) manager ->
        CM.on_session_expired manager (fun () ->
            expired_called := true ;
            Lwt.return_unit ) ;
        let* client =
          CM.login manager ~identifier:"test@example.com" ~password:"secret" ()
        in
        (* making an API call should trigger token refresh which will fail *)
        Lwt.catch
          (fun () ->
            let* _ =
              C.query client "test.endpoint" (`Assoc []) (fun _ -> Ok ())
            in
            fail "should have raised SessionExpired" )
          (function
            | Hermes.Xrpc_error {error= "SessionExpired"; _} ->
                Lwt.return_unit
            | e ->
                Lwt.reraise e ) )
    |> Lwt.map fst
  in
  check bool "expired callback was called" true !expired_called ;
  Lwt.return_unit

(** tests *)

let login_tests =
  [ ("login success", `Quick, test_login_success)
  ; ("login error", `Quick, test_login_error)
  ; ("login with auth factor", `Quick, test_login_with_auth_factor) ]

let resume_tests = [("resume session", `Quick, test_resume_session)]

let callback_tests =
  [ ("session update callback", `Quick, test_session_update_callback)
  ; ("session expired callback", `Quick, test_session_expired_callback) ]

let logout_tests =
  [ ("logout success", `Quick, test_logout_success)
  ; ("logout no session", `Quick, test_logout_no_session) ]

let session_tests =
  [ ("get session before login", `Quick, test_get_session_before_login)
  ; ("get session after login", `Quick, test_get_session_after_login) ]

let refresh_tests =
  [ ("automatic token refresh", `Quick, test_automatic_token_refresh)
  ; ( "refresh failure clears session"
    , `Quick
    , test_refresh_failure_clears_session ) ]

let () =
  run "Credential_manager"
    [ ("login", login_tests)
    ; ("resume", resume_tests)
    ; ("callbacks", callback_tests)
    ; ("logout", logout_tests)
    ; ("session", session_tests)
    ; ("refresh", refresh_tests) ]
