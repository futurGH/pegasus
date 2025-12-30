open Alcotest

(** helpers *)
let test_string = testable Fmt.string String.equal

let test_int64 = testable Fmt.int64 Int64.equal

(* blob tests *)
let test_blob_to_yojson () =
  let cid =
    Cid.of_string "bafyreib7k7m7h7x6qrvxqrwqe2m6q5p5zklvp4fqq2g6hh6h6t6x6x6x6y"
  in
  match cid with
  | Ok cid -> (
      let blob : Hermes.blob =
        {type_= "blob"; ref= cid; mime_type= "image/png"; size= 12345L}
      in
      let json = Hermes.blob_to_yojson blob in
      let json_str = Yojson.Safe.to_string json in
      check bool "contains mimeType" true
        (String.length json_str > 0 && String.sub json_str 0 1 = "{") ;
      (* verify valid JSON with expected structure *)
      match json with
      | `Assoc pairs ->
          check bool "has mimeType key" true
            (List.exists (fun (k, _) -> k = "mimeType") pairs) ;
          check bool "has size key" true
            (List.exists (fun (k, _) -> k = "size") pairs)
      | _ ->
          fail "expected object" )
  | Error _ ->
      fail "couldn't parse cid constant"

let test_blob_roundtrip () =
  let cid =
    Cid.of_string "bafyreib7k7m7h7x6qrvxqrwqe2m6q5p5zklvp4fqq2g6hh6h6t6x6x6x6y"
  in
  match cid with
  | Ok cid -> (
      let original : Hermes.blob =
        {type_= "blob"; ref= cid; mime_type= "image/jpeg"; size= 54321L}
      in
      let json = Hermes.blob_to_yojson original in
      match Hermes.blob_of_yojson json with
      | Ok decoded ->
          check test_string "mime_type matches" original.mime_type
            decoded.mime_type ;
          check test_int64 "size matches" original.size decoded.size
      | Error e ->
          fail ("roundtrip failed: " ^ e) )
  | Error _ ->
      ()

(** session tests *)
let test_session_to_yojson () =
  let session : Hermes.session =
    { access_jwt= "eyJ..."
    ; refresh_jwt= "eyJ..."
    ; did= "did:plc:example"
    ; handle= "user.bsky.social"
    ; pds_uri= Some "https://pds.example.com"
    ; email= Some "user@example.com"
    ; email_confirmed= Some true
    ; email_auth_factor= Some false
    ; active= Some true
    ; status= None }
  in
  let json = Hermes.session_to_yojson session in
  match json with
  | `Assoc pairs ->
      check bool "has accessJwt" true
        (List.exists (fun (k, _) -> k = "accessJwt") pairs) ;
      check bool "has refreshJwt" true
        (List.exists (fun (k, _) -> k = "refreshJwt") pairs) ;
      check bool "has did" true (List.exists (fun (k, _) -> k = "did") pairs) ;
      check bool "has handle" true
        (List.exists (fun (k, _) -> k = "handle") pairs)
  | _ ->
      fail "expected object"

let test_session_of_yojson_full () =
  let json =
    Yojson.Safe.from_string
      {|{
    "accessJwt": "access_token",
    "refreshJwt": "refresh_token",
    "did": "did:plc:test",
    "handle": "test.bsky.social",
    "pdsUri": "https://pds.test.com",
    "email": "test@example.com",
    "emailConfirmed": true,
    "emailAuthFactor": false,
    "active": true,
    "status": "active"
  }|}
  in
  match Hermes.session_of_yojson json with
  | Ok session ->
      check test_string "access_jwt" "access_token" session.access_jwt ;
      check test_string "refresh_jwt" "refresh_token" session.refresh_jwt ;
      check test_string "did" "did:plc:test" session.did ;
      check test_string "handle" "test.bsky.social" session.handle ;
      check (option test_string) "pds_uri" (Some "https://pds.test.com")
        session.pds_uri ;
      check (option test_string) "email" (Some "test@example.com") session.email ;
      check (option bool) "email_confirmed" (Some true) session.email_confirmed ;
      check (option bool) "email_auth_factor" (Some false)
        session.email_auth_factor ;
      check (option bool) "active" (Some true) session.active ;
      check (option test_string) "status" (Some "active") session.status
  | Error e ->
      fail ("parse failed: " ^ e)

let test_session_of_yojson_minimal () =
  let json =
    Yojson.Safe.from_string
      {|{
    "accessJwt": "access_token",
    "refreshJwt": "refresh_token",
    "did": "did:plc:test",
    "handle": "test.bsky.social"
  }|}
  in
  match Hermes.session_of_yojson json with
  | Ok session ->
      check test_string "access_jwt" "access_token" session.access_jwt ;
      check test_string "did" "did:plc:test" session.did ;
      check (option test_string) "pds_uri" None session.pds_uri ;
      check (option test_string) "email" None session.email ;
      check (option bool) "active" None session.active
  | Error e ->
      fail ("parse failed: " ^ e)

let test_session_roundtrip () =
  let original : Hermes.session =
    { access_jwt= "access123"
    ; refresh_jwt= "refresh456"
    ; did= "did:plc:roundtrip"
    ; handle= "roundtrip.test"
    ; pds_uri= None
    ; email= Some "rt@test.com"
    ; email_confirmed= None
    ; email_auth_factor= None
    ; active= Some true
    ; status= None }
  in
  let json = Hermes.session_to_yojson original in
  match Hermes.session_of_yojson json with
  | Ok decoded ->
      check test_string "access_jwt" original.access_jwt decoded.access_jwt ;
      check test_string "refresh_jwt" original.refresh_jwt decoded.refresh_jwt ;
      check test_string "did" original.did decoded.did ;
      check test_string "handle" original.handle decoded.handle ;
      check (option test_string) "pds_uri" original.pds_uri decoded.pds_uri ;
      check (option test_string) "email" original.email decoded.email ;
      check (option bool) "active" original.active decoded.active
  | Error e ->
      fail ("roundtrip failed: " ^ e)

(** tests *)

let blob_tests =
  [ ("blob_to_yojson", `Quick, test_blob_to_yojson)
  ; ("blob roundtrip", `Quick, test_blob_roundtrip) ]

let session_tests =
  [ ("session_to_yojson", `Quick, test_session_to_yojson)
  ; ("session_of_yojson full", `Quick, test_session_of_yojson_full)
  ; ("session_of_yojson minimal", `Quick, test_session_of_yojson_minimal)
  ; ("session roundtrip", `Quick, test_session_roundtrip) ]

let () = run "Types" [("blob", blob_tests); ("session", session_tests)]
