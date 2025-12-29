open Alcotest

(** helpers *)
let test_string = testable Fmt.string String.equal

(* create a minimal jwt
   we only care about the (base64url encoded json) payload, so header and signature can be anything *)
let make_jwt payload_json =
  let header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" in
  (* {"alg":"HS256","typ":"JWT"} *)
  let payload =
    Base64.encode_string ~alphabet:Base64.uri_safe_alphabet ~pad:false
      payload_json
  in
  let signature = "signature" in
  header ^ "." ^ payload ^ "." ^ signature

(* decoding a valid JWT *)
let test_decode_valid () =
  let now = int_of_float (Unix.time ()) in
  let payload_json =
    Printf.sprintf {|{"sub":"did:plc:test","iat":%d,"exp":%d}|} now (now + 3600)
  in
  let jwt = make_jwt payload_json in
  match Hermes.Jwt.decode_payload jwt with
  | Ok payload ->
      check (option test_string) "sub" (Some "did:plc:test") payload.sub ;
      check (option int) "iat" (Some now) payload.iat ;
      check (option int) "exp" (Some (now + 3600)) payload.exp
  | Error e ->
      fail ("decode failed: " ^ e)

(* decoding JWT with additional fields *)
let test_decode_with_extra_fields () =
  let now = int_of_float (Unix.time ()) in
  let payload_json =
    Printf.sprintf
      {|{"sub":"did:plc:extra","iat":%d,"exp":%d,"scope":"atproto","aud":"did:web:server"}|}
      now (now + 3600)
  in
  let jwt = make_jwt payload_json in
  match Hermes.Jwt.decode_payload jwt with
  | Ok payload ->
      check (option test_string) "sub" (Some "did:plc:extra") payload.sub ;
      check (option int) "exp" (Some (now + 3600)) payload.exp ;
      check (option test_string) "aud" (Some "did:web:server") payload.aud
  | Error e ->
      fail ("decode failed: " ^ e)

(* decoding invalid JWT format *)
let test_decode_invalid_format () =
  let invalid = "not.a.jwt.with.wrong.parts" in
  match Hermes.Jwt.decode_payload invalid with
  | Ok _ ->
      fail "should have failed"
  | Error e ->
      check bool "has error" true (String.length e > 0)

let test_decode_no_dots () =
  let invalid = "nodots" in
  match Hermes.Jwt.decode_payload invalid with
  | Ok _ ->
      fail "should have failed"
  | Error _ ->
      ()

(* decoding JWT with invalid base64 *)
let test_decode_invalid_base64 () =
  let invalid = "header.!!!invalid!!!.signature" in
  match Hermes.Jwt.decode_payload invalid with
  | Ok _ ->
      fail "should have failed"
  | Error _ ->
      ()

(* decoding JWT with invalid JSON payload *)
let test_decode_invalid_json () =
  let header = "eyJhbGciOiJIUzI1NiJ9" in
  let payload =
    Base64.encode_string ~alphabet:Base64.uri_safe_alphabet ~pad:false
      "not json"
  in
  let jwt = header ^ "." ^ payload ^ ".sig" in
  match Hermes.Jwt.decode_payload jwt with
  | Ok _ ->
      fail "should have failed"
  | Error _ ->
      ()

(* test is_expired with expired token *)
let test_expired_token () =
  let past = int_of_float (Unix.time ()) - 3600 in
  (* 1 hour ago *)
  let payload_json =
    Printf.sprintf {|{"sub":"test","iat":%d,"exp":%d}|} past past
  in
  let jwt = make_jwt payload_json in
  check bool "is expired" true (Hermes.Jwt.is_expired jwt)

(* test is_expired with valid token *)
let test_valid_token () =
  let now = int_of_float (Unix.time ()) in
  let future = now + 3600 in
  (* 1 hour from now *)
  let payload_json =
    Printf.sprintf {|{"sub":"test","iat":%d,"exp":%d}|} now future
  in
  let jwt = make_jwt payload_json in
  check bool "is not expired" false (Hermes.Jwt.is_expired jwt)

(* test is_expired with buffer *)
let test_expired_with_buffer () =
  let now = int_of_float (Unix.time ()) in
  let almost_expired = now + 30 in
  (* expires in 30 seconds *)
  let payload_json =
    Printf.sprintf {|{"sub":"test","iat":%d,"exp":%d}|} now almost_expired
  in
  let jwt = make_jwt payload_json in
  (* default buffer is 60 seconds, so this should be considered expired *)
  check bool "expired with buffer" true (Hermes.Jwt.is_expired jwt)

(* is_expired with invalid JWT returns true *)
let test_expired_invalid_jwt () =
  check bool "invalid JWT treated as expired" true
    (Hermes.Jwt.is_expired "invalid")

(* test get_expiration *)
let test_get_expiration () =
  let now = int_of_float (Unix.time ()) in
  let exp_time = now + 3600 in
  let payload_json = Printf.sprintf {|{"sub":"test","exp":%d}|} exp_time in
  let jwt = make_jwt payload_json in
  check (option int) "expiration" (Some exp_time)
    (Hermes.Jwt.get_expiration jwt)

let test_get_expiration_missing () =
  let payload_json = {|{"sub":"test"}|} in
  let jwt = make_jwt payload_json in
  check (option int) "no expiration" None (Hermes.Jwt.get_expiration jwt)

(** tests *)

let decode_tests =
  [ ("decode valid JWT", `Quick, test_decode_valid)
  ; ("decode with extra fields", `Quick, test_decode_with_extra_fields)
  ; ("decode invalid format", `Quick, test_decode_invalid_format)
  ; ("decode no dots", `Quick, test_decode_no_dots)
  ; ("decode invalid base64", `Quick, test_decode_invalid_base64)
  ; ("decode invalid json", `Quick, test_decode_invalid_json) ]

let expiry_tests =
  [ ("expired token", `Quick, test_expired_token)
  ; ("valid token", `Quick, test_valid_token)
  ; ("expired with buffer", `Quick, test_expired_with_buffer)
  ; ("invalid JWT is expired", `Quick, test_expired_invalid_jwt)
  ; ("get_expiration", `Quick, test_get_expiration)
  ; ("get_expiration missing", `Quick, test_get_expiration_missing) ]

let () = run "Jwt" [("decode", decode_tests); ("expiry", expiry_tests)]
