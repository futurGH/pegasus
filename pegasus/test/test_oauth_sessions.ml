open Alcotest
open Pegasus
open Yojson.Safe.Util

let did = "did:plc:oauthsessiontest"

let client_id = "https://client.invalid/metadata.json"

let jwk =
  let key, _ = Env.jwt_pubkey in
  let raw = Kleidos.K256.normalize_pubkey_to_raw key in
  Oauth.Dpop.
    { crv= "secp256k1"
    ; kty= "EC"
    ; x= Jwt.b64_encode (Bytes.sub_string raw 0 32)
    ; y= Jwt.b64_encode (Bytes.sub_string raw 32 32) }

let jkt = Oauth.Dpop.compute_jwk_thumbprint jwk

let dpop ~method_ ~target ?token () =
  let header =
    `Assoc
      [ ("typ", `String "dpop+jwt")
      ; ("alg", `String "ES256K")
      ; ("jwk", Oauth.Dpop.ec_jwk_to_yojson jwk) ]
  in
  let claims =
    [ ("jti", `String (Oauth.Token.next_token_id "proof-"))
    ; ("htm", `String method_)
    ; ("htu", `String (Env.host_endpoint ^ target))
    ; ("iat", `Int (Util.Time.now_s ()))
    ; ("nonce", `String (Oauth.Dpop.next_nonce ())) ]
    @
    match token with
    | None ->
        []
    | Some token ->
        [ ( "ath"
          , `String
              Digestif.SHA256.(
                digest_string token |> to_raw_string |> Jwt.b64_encode ) ) ]
  in
  let input =
    Jwt.b64_encode (Yojson.Safe.to_string header)
    ^ "."
    ^ Jwt.b64_encode (Yojson.Safe.to_string (`Assoc claims))
  in
  input ^ "."
  ^ Jwt.b64_encode
      (Bytes.to_string
         (Kleidos.sign ~privkey:Env.jwt_key ~msg:(Bytes.of_string input)) )

let with_db f () =
  let path = Filename.temp_file "pegasus-oauth-test" ".db" in
  Lwt_main.run
    (let%lwt db =
       Util.Sqlite.connect ~create:true (Uri.of_string ("sqlite3://" ^ path))
     in
     Lwt.finalize
       (fun () ->
         let%lwt () = Migrations.run_migrations Data_store db in
         let%lwt () =
           Data_store.create_actor db ~did ~handle:"oauth-test.invalid"
             ~email:"test@example.invalid" ~password:"test-only"
             ~signing_key:(Kleidos.privkey_to_multikey Env.jwt_key)
         in
         f db )
       (fun () ->
         let%lwt () = Caqti_lwt_unix.Pool.drain db in
         List.iter
           (fun suffix ->
             try Sys.remove (path ^ suffix) with Sys_error _ -> () )
           [""; "-wal"; "-shm"] ;
         Lwt.return_unit ) )

let post_token db fields =
  let target = "/oauth/token" in
  let req =
    Dream.request ~method_:`POST ~target
      ~headers:
        [ ("Content-Type", "application/json")
        ; ("DPoP", dpop ~method_:"POST" ~target ()) ]
      (Yojson.Safe.to_string
         (`Assoc (("client_id", `String client_id) :: fields)) )
  in
  let%lwt response = Api.Oauth_.Token.post_handler {req; db} in
  let%lwt body = Dream.body response in
  Lwt.return
    (Dream.status_to_int (Dream.status response), Yojson.Safe.from_string body)

let issue db scope =
  let now = Util.Time.now_ms () in
  let request_data =
    `Assoc
      [ ("client_id", `String client_id)
      ; ("response_type", `String "code")
      ; ("redirect_uri", `String "https://client.invalid/callback")
      ; ("scope", `String scope)
      ; ("state", `String "test")
      ; ( "code_challenge"
        , `String
            Digestif.SHA256.(
              digest_string "verifier" |> to_raw_string |> Jwt.b64_encode ) )
      ; ("code_challenge_method", `String "S256") ]
    |> Yojson.Safe.to_string
  in
  let%lwt () =
    Oauth.Queries.insert_par_request db
      { request_id= "request"
      ; client_id
      ; request_data
      ; dpop_jkt= Some jkt
      ; expires_at= now + 60000
      ; created_at= now }
  in
  let%lwt () =
    Oauth.Queries.insert_auth_code db
      { code= "code"
      ; request_id= "request"
      ; authorized_by= Some did
      ; authorized_at= Some now
      ; authorized_ip= None
      ; authorized_user_agent= None
      ; expires_at= now + 60000
      ; used= false }
  in
  let%lwt status, response =
    post_token db
      [ ("grant_type", `String "authorization_code")
      ; ("code", `String "code")
      ; ("redirect_uri", `String "https://client.invalid/callback")
      ; ("code_verifier", `String "verifier") ]
  in
  check int "authorization code exchange" 200 status ;
  Lwt.return response

let access db ?(collection = "app.example.allowed") token =
  let target = "/xrpc/com.atproto.repo.createRecord" in
  let req =
    Dream.request ~method_:`POST ~target
      ~headers:
        [ ("Authorization", "DPoP " ^ token)
        ; ("DPoP", dpop ~method_:"POST" ~target ~token ()) ]
      ""
  in
  let%lwt response =
    Xrpc.handler ~auth:OAuth
      (fun ctx ->
        Auth.assert_repo_scope ctx.auth ~collection ~action:Create ;
        Dream.empty `OK )
      {req; db}
  in
  Lwt.return (Dream.status_to_int (Dream.status response))

let refresh db token =
  post_token db
    [("grant_type", `String "refresh_token"); ("refresh_token", `String token)]

let claim token name =
  let _, claims = Result.get_ok (Jwt.decode_jwt token) in
  claims |> member name

let rewrite token replacements =
  let _, claims = Result.get_ok (Jwt.decode_jwt token) in
  let fields =
    to_assoc claims
    |> List.filter (fun (key, _) -> not (List.mem_assoc key replacements))
  in
  Jwt.sign_jwt ~typ:"at+jwt" ~signing_key:Env.jwt_key
    (`Assoc
       (List.filter (fun (_, value) -> value <> `Null) replacements @ fields) )

let test_large_grants db =
  let scope =
    "atproto repo:app.example.allowed?action=create "
    ^ String.concat " "
        (List.init 400 (fun i ->
             Printf.sprintf "repo:app.example.collection%d?action=create" i ) )
  in
  let%lwt response = issue db scope in
  let access_token = response |> member "access_token" |> to_string in
  let refresh_token = response |> member "refresh_token" |> to_string in
  check bool "large grant" true
    (String.length (response |> member "scope" |> to_string) > 16000) ;
  check bool "token fits ordinary HTTP headers" true
    (String.length access_token < 1024) ;
  let%lwt status = access db access_token in
  check int "granted operation" 200 status ;
  let%lwt status =
    access db ~collection:"app.example.collection399" access_token
  in
  check int "permissions at the end of a large grant" 200 status ;
  let%lwt status = access db ~collection:"app.example.denied" access_token in
  check int "ungranted operation" 401 status ;
  let%lwt status, refreshed = refresh db refresh_token in
  check int "refresh succeeds" 200 status ;
  let new_access = refreshed |> member "access_token" |> to_string in
  check string "permissions survive refresh"
    (response |> member "scope" |> to_string)
    (refreshed |> member "scope" |> to_string) ;
  let%lwt old_status = access db access_token in
  let%lwt new_status = access db new_access in
  check (pair int int) "both access tokens survive rotation" (200, 200)
    (old_status, new_status) ;
  let%lwt reused_status, _ = refresh db refresh_token in
  check int "old refresh token is single use" 400 reused_status ;
  let%lwt () = Oauth.Queries.delete_oauth_tokens_by_client db ~did ~client_id in
  let%lwt status = access db new_access in
  check int "revocation takes effect" 401 status ;
  Lwt.return_unit

let test_compatibility_and_binding db =
  let%lwt response =
    issue db "atproto repo:app.example.allowed?action=create"
  in
  let token = response |> member "access_token" |> to_string in
  (* access-token format used before session references *)
  let legacy =
    rewrite token [("sid", `Null); ("scope", response |> member "scope")]
  in
  let%lwt status = access db legacy in
  check int "legacy token still works" 200 status ;
  let%lwt status, refreshed =
    refresh db (response |> member "refresh_token" |> to_string)
  in
  check int "existing session refreshes" 200 status ;
  let%lwt status =
    access db (refreshed |> member "access_token" |> to_string)
  in
  check int "refreshed token works" 200 status ;
  let%lwt () =
    Lwt_list.iter_s
      (fun (name, replacements) ->
        let%lwt status = access db (rewrite token replacements) in
        check bool name true (status >= 400 && status < 500) ;
        Lwt.return_unit )
      [ ( "session cannot authorize a different user"
        , [("sub", `String "did:plc:other")] )
      ; ( "session must match the proof key"
        , [("cnf", `Assoc [("jkt", `String "other-key")])] )
      ; ( "missing session cannot fall back to embedded scopes"
        , [("sid", `String "missing"); ("scope", response |> member "scope")] )
      ; ("expired access token", [("exp", `Int (Util.Time.now_s () - 1))]) ]
  in
  let%lwt () =
    Util.Sqlite.use_pool db (fun conn ->
        Migrations.execute_raw conn
          "UPDATE oauth_tokens SET dpop_jkt = 'different-session-key'" )
  in
  let%lwt status = access db token in
  check int "session key must match the token and proof" 401 status ;
  let%lwt () =
    Util.Sqlite.use_pool db (fun conn ->
        Migrations.execute_raw conn
          ( "UPDATE oauth_tokens SET session_expires_at = 0, dpop_jkt = '" ^ jkt
          ^ "'" ) )
  in
  let%lwt status = access db token in
  check int "expired session" 401 status ;
  Lwt.return_unit

let () =
  Mirage_crypto_rng_unix.use_default () ;
  run "oauth sessions"
    [ ( "behavior"
      , [ ( "large grants, refresh, revocation"
          , `Quick
          , with_db test_large_grants )
        ; ( "compatibility and session binding"
          , `Quick
          , with_db test_compatibility_and_binding ) ] ) ]
