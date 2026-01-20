type nonce_state =
  { secret: bytes
  ; mutable counter: int
  ; mutable prev: string
  ; mutable curr: string
  ; mutable next: string }

type ec_jwk = {crv: string; kty: string; x: string; y: string}
[@@deriving yojson {strict= false}]

type proof = {jti: string; jkt: string; htm: string; htu: string}
[@@deriving yojson {strict= false}]

let jti_cache : (string, int) Hashtbl.t =
  Hashtbl.create Constants.jti_cache_size

let cleanup_jti_cache () =
  let now = Util.Time.now_ms () in
  Hashtbl.filter_map_inplace
    (fun _ expires_at -> if expires_at > now then Some expires_at else None)
    jti_cache

let revocation_cache : (string, int) Hashtbl.t = Hashtbl.create 1000

let cleanup_revocation_cache () =
  let now_s = int_of_float (Unix.gettimeofday ()) in
  let max_token_age_s = Constants.access_token_expiry_ms / 1000 in
  Hashtbl.filter_map_inplace
    (fun _ revoked_at ->
      if now_s - revoked_at < max_token_age_s then Some revoked_at else None )
    revocation_cache

let revoke_tokens_for_did did =
  let now_s = int_of_float (Unix.gettimeofday ()) in
  Hashtbl.replace revocation_cache did now_s ;
  if Hashtbl.length revocation_cache mod 50 = 0 then cleanup_revocation_cache ()

let is_token_revoked ~did ~iat =
  match Hashtbl.find_opt revocation_cache did with
  | Some revoked_at ->
      iat < revoked_at
  | None ->
      false

let compute_nonce secret counter =
  let data = Bytes.create 8 in
  Bytes.set_int64_be data 0 (Int64.of_int counter) ;
  Digestif.SHA256.(
    hmac_bytes ~key:(Bytes.to_string secret) data
    |> to_raw_string |> Jwt.b64_encode )

let create_nonce_state secret =
  let counter = Util.Time.now_ms () / Constants.dpop_rotation_interval_ms in
  { secret
  ; counter
  ; prev= compute_nonce secret (pred counter)
  ; curr= compute_nonce secret counter
  ; next= compute_nonce secret (succ counter) }

let nonce_state = ref (create_nonce_state Env.dpop_nonce_secret)

let next_nonce () =
  let now_counter = Util.Time.now_ms () / Constants.dpop_rotation_interval_ms in
  let diff = now_counter - !nonce_state.counter in
  ( match diff with
  | 0 ->
      ()
  | 1 ->
      !nonce_state.prev <- !nonce_state.curr ;
      !nonce_state.curr <- !nonce_state.next ;
      !nonce_state.next <- compute_nonce !nonce_state.secret (succ now_counter)
  | 2 ->
      !nonce_state.prev <- !nonce_state.next ;
      !nonce_state.curr <- compute_nonce !nonce_state.secret now_counter ;
      !nonce_state.next <- compute_nonce !nonce_state.secret (succ now_counter)
  | _ ->
      !nonce_state.prev <- compute_nonce !nonce_state.secret (pred now_counter) ;
      !nonce_state.curr <- compute_nonce !nonce_state.secret now_counter ;
      !nonce_state.next <- compute_nonce !nonce_state.secret (succ now_counter)
  ) ;
  !nonce_state.counter <- now_counter ;
  !nonce_state.next

let verify_nonce nonce =
  let _ = next_nonce () in
  nonce = !nonce_state.prev || nonce = !nonce_state.curr
  || nonce = !nonce_state.next

let add_jti jti =
  let expires_at = int_of_float (Unix.gettimeofday ()) + Constants.jti_ttl_s in
  if Hashtbl.mem jti_cache jti then false (* replay *)
  else (
    Hashtbl.add jti_cache jti expires_at ;
    (* clean up every once in a while *)
    if Hashtbl.length jti_cache mod 100 = 0 then cleanup_jti_cache () ;
    true )

let is_loopback host =
  host = "127.0.0.1" || host = "[::1]" || host = "localhost"

let normalize_url url =
  let uri = Uri.of_string url in
  let host = Uri.host uri in
  let scheme, normalized_host =
    match host with
    | Some h when is_loopback h ->
        ("http", h)
    | Some h ->
        ("https", h)
    | None ->
        ("https", Env.hostname)
  in
  Uri.make ~scheme ~host:normalized_host ~path:(Uri.path uri) ()
  |> Uri.to_string

let compute_jwk_thumbprint jwk =
  let {crv; kty; x; y} = jwk in
  let tp =
    (* keys must be in lexicographic order *)
    Printf.sprintf {|{"crv":"%s","kty":"%s","x":"%s","y":"%s"}|} crv kty x y
  in
  Digestif.SHA256.(digest_string tp |> to_raw_string |> Jwt.b64_encode)

let verify_signature jwt jwk =
  let parts = String.split_on_char '.' jwt in
  match parts with
  | [header_b64; payload_b64; sig_b64] ->
      let signing_input = header_b64 ^ "." ^ payload_b64 in
      let msg = Bytes.of_string signing_input in
      let {x; y; crv; _} = jwk in
      let x = x |> Jwt.b64_decode |> Bytes.of_string in
      let y = y |> Jwt.b64_decode |> Bytes.of_string in
      let pubkey = Bytes.cat (Bytes.of_string "\x04") (Bytes.cat x y) in
      let sig_bytes = Jwt.b64_decode sig_b64 |> Bytes.of_string in
      let r = Bytes.sub sig_bytes 0 32 in
      let s = Bytes.sub sig_bytes 32 32 in
      let signature = Bytes.cat r s in
      let pubkey, signature =
        match crv with
        | "secp256k1" ->
            ( (pubkey, (module Kleidos.K256 : Kleidos.CURVE))
            , Kleidos.K256.low_s_normalize_signature signature )
        | "P-256" ->
            ( (pubkey, (module Kleidos.P256 : Kleidos.CURVE))
            , Kleidos.P256.low_s_normalize_signature signature )
        | _ ->
            failwith "unsupported algorithm"
      in
      Kleidos.verify ~pubkey ~msg ~signature
  | _ ->
      false

let verify_dpop_proof ~mthd ~url ~dpop_header ?access_token () =
  match dpop_header with
  | None ->
      Error "missing dpop header"
  | Some jwt -> (
      let open Yojson.Safe.Util in
      match String.split_on_char '.' jwt with
      | [header_b64; payload_b64; _] -> (
          let header = Yojson.Safe.from_string (Jwt.b64_decode header_b64) in
          let payload = Yojson.Safe.from_string (Jwt.b64_decode payload_b64) in
          let typ = header |> member "typ" |> to_string in
          if typ <> "dpop+jwt" then Error "invalid typ in dpop proof"
          else
            let alg = header |> member "alg" |> to_string in
            if alg <> "ES256" && alg <> "ES256K" then
              Error "only es256 and es256k supported for dpop"
            else
              match header |> member "jwk" |> ec_jwk_of_yojson with
              | Error e ->
                  Log.debug (fun log -> log "error parsing jwk: %s" e) ;
                  Errors.internal_error ()
              | Ok jwk -> (
                  if
                    not
                      ( match (alg, jwk.crv) with
                      | "ES256", "P-256" ->
                          true
                      | "ES256K", "secp256k1" ->
                          true
                      | _ ->
                          false )
                  then
                    Error
                      (Printf.sprintf "algorithm %s doesn't match curve %s" alg
                         jwk.crv )
                  else
                    let jti = payload |> member "jti" |> to_string in
                    let htm = payload |> member "htm" |> to_string in
                    let htu = payload |> member "htu" |> to_string in
                    let iat = payload |> member "iat" |> to_int in
                    let nonce_claim =
                      payload |> member "nonce" |> to_string_option
                    in
                    match nonce_claim with
                    (* error must be this string; see https://datatracker.ietf.org/doc/html/rfc9449#section-8 *)
                    | None ->
                        Error "use_dpop_nonce"
                    | Some n when not (verify_nonce n) ->
                        Log.debug (fun log ->
                            let state = !nonce_state in
                            log
                              "given nonce %s, failed to match any of: %s, %s, \
                               %s"
                              n state.prev state.curr state.next ) ;
                        Error "use_dpop_nonce"
                    | Some _ -> (
                        if htm <> mthd then Error "htm mismatch"
                        else if
                          not
                            (String.equal (normalize_url htu)
                               (normalize_url url) )
                        then Error "htu mismatch"
                        else
                          let now = int_of_float (Unix.gettimeofday ()) in
                          if now - iat > Constants.max_dpop_age_s then
                            Error "dpop proof too old"
                          else if iat - now > 5 then
                            Error "dpop proof in future"
                          else if not (add_jti jti) then
                            Error "dpop proof replay detected"
                          else if
                            not (try verify_signature jwt jwk with _ -> false)
                          then Error "invalid dpop signature"
                          else
                            let jkt = compute_jwk_thumbprint jwk in
                            (* verify ath if access token is provided *)
                            match access_token with
                            | Some token ->
                                let ath_claim =
                                  payload |> member "ath" |> to_string_option
                                in
                                let expected_ath =
                                  Digestif.SHA256.(
                                    digest_string token |> to_raw_string
                                    |> Jwt.b64_encode )
                                in
                                if Some expected_ath <> ath_claim then
                                  Error "ath mismatch"
                                else Ok {jti; jkt; htm; htu}
                            | None ->
                                let ath_claim =
                                  payload |> member "ath" |> to_string_option
                                in
                                if ath_claim <> None then
                                  Error
                                    "ath claim not allowed without access token"
                                else Ok {jti; jkt; htm; htu} ) ) )
      | _ ->
          Error "invalid dpop jwt" )
