type nonce_state =
  { secret: bytes
  ; mutable counter: int64
  ; mutable prev: string
  ; mutable curr: string
  ; mutable next: string
  ; rotation_interval_ms: int64 }

type ec_jwk = {crv: string; kty: string; x: string; y: string}
[@@deriving yojson]

type proof = {jti: string; jkt: string; htm: string; htu: string}
[@@deriving yojson]

let jti_cache : (string, int) Hashtbl.t =
  Hashtbl.create Constants.jti_cache_size

let cleanup_jti_cache () =
  let now = int_of_float (Unix.gettimeofday ()) in
  Hashtbl.filter_map_inplace
    (fun _ expires_at -> if expires_at > now then Some expires_at else None)
    jti_cache

let compute_nonce secret counter =
  let data = Bytes.create 8 in
  Bytes.set_int64_be data 0 counter ;
  Digestif.SHA256.(
    hmac_bytes ~key:(Bytes.to_string secret) data
    |> to_raw_string |> Jwt.b64_encode )

let create_nonce_state secret =
  let counter =
    Int64.div
      (Int64.of_float (Unix.gettimeofday () *. 1000.))
      Constants.dpop_rotation_interval_ms
  in
  { secret
  ; counter
  ; prev= compute_nonce secret (Int64.pred counter)
  ; curr= compute_nonce secret counter
  ; next= compute_nonce secret (Int64.succ counter)
  ; rotation_interval_ms= Constants.dpop_rotation_interval_ms }

let nonce_state = ref (create_nonce_state Env.dpop_nonce_secret)

let next_nonce () =
  let now_counter =
    Int64.div
      (Int64.of_float (Unix.gettimeofday () *. 1000.))
      !nonce_state.rotation_interval_ms
  in
  if now_counter <> !nonce_state.counter then (
    !nonce_state.prev <- !nonce_state.curr ;
    !nonce_state.curr <- !nonce_state.next ;
    !nonce_state.next <-
      compute_nonce !nonce_state.secret (Int64.succ now_counter) ;
    !nonce_state.counter <- now_counter ) ;
  !nonce_state.next

let verify_nonce nonce =
  let valid =
    nonce = !nonce_state.prev || nonce = !nonce_state.curr
    || nonce = !nonce_state.next
  in
  ignore next_nonce ; valid

let add_jti jti =
  let expires_at = int_of_float (Unix.gettimeofday ()) + Constants.jti_ttl_s in
  if Hashtbl.mem jti_cache jti then false (* replay *)
  else (
    Hashtbl.add jti_cache jti expires_at ;
    (* clean up every once in a while *)
    if Hashtbl.length jti_cache mod 100 = 0 then cleanup_jti_cache () ;
    true )

let normalize_url url =
  let uri = Uri.of_string url in
  Uri.make ~scheme:"https"
    ~host:(Uri.host uri |> Option.value ~default:Env.hostname)
    ~path:(Uri.path uri) ()
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
      let pubkey =
        ( pubkey
        , match crv with
          | "secp256k1" ->
              (module Kleidos.K256 : Kleidos.CURVE)
          | "P-256" ->
              (module Kleidos.P256 : Kleidos.CURVE)
          | _ ->
              failwith "unsupported algorithm" )
      in
      let sig_bytes = Jwt.b64_decode sig_b64 |> Bytes.of_string in
      let r = Bytes.sub sig_bytes 0 32 in
      let s = Bytes.sub sig_bytes 32 32 in
      let signature = Bytes.cat r s in
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
              let jwk =
                header |> member "jwk" |> ec_jwk_of_yojson |> Result.get_ok
              in
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
                    Error "use_dpop_nonce"
                | Some _ -> (
                    if htm <> mthd then Error "htm mismatch"
                    else if
                      not (String.equal (normalize_url htu) (normalize_url url))
                    then Error "htu mismatch"
                    else
                      let now = int_of_float (Unix.gettimeofday ()) in
                      if now - iat > Constants.max_dpop_age_s then
                        Error "dpop proof too old"
                      else if iat - now > 5 then Error "dpop proof in future"
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
                              Error "ath claim not allowed without access token"
                            else Ok {jti; jkt; htm; htu} ) )
      | _ ->
          Error "invalid dpop jwt" )
