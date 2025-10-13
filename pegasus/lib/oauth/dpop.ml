type nonce_state =
  { secret: bytes
  ; mutable counter: int64
  ; mutable prev: string
  ; mutable curr: string
  ; mutable next: string
  ; rotation_interval_ms: int64 }

type proof = {jti: string; jkt: string; htm: string; htu: string}

type context = {nonce_state: nonce_state; jti_cache: (string, int) Hashtbl.t}

let create_nonce_state ?(rotation_interval_ms = 60_000L) secret =
  let counter =
    Int64.div
      (Int64.of_float (Unix.gettimeofday () *. 1000.))
      rotation_interval_ms
  in
  let compute_nonce cnt =
    let data = Bytes.create 8 in
    Bytes.set_int64_be data 0 cnt ;
    Digestif.SHA256.(
      hmac_bytes ~key:(Bytes.to_string secret) data
      |> to_raw_string
      |> Base64.encode_exn ~pad:false )
  in
  { secret
  ; counter
  ; prev= compute_nonce (Int64.pred counter)
  ; curr= compute_nonce counter
  ; next= compute_nonce (Int64.succ counter)
  ; rotation_interval_ms }

let next_nonce state =
  let now_counter =
    Int64.div
      (Int64.of_float (Unix.gettimeofday () *. 1000.))
      state.rotation_interval_ms
  in
  if now_counter <> state.counter then (
    state.prev <- state.curr ;
    state.curr <- state.next ;
    let data = Bytes.create 8 in
    Bytes.set_int64_be data 0 (Int64.succ now_counter) ;
    state.next <-
      Digestif.SHA256.(
        hmac_bytes ~key:(Bytes.to_string state.secret) data
        |> to_raw_string
        |> Base64.encode_exn ~pad:false ) ;
    state.counter <- now_counter ) ;
  state.next

let verify_nonce state nonce =
  nonce = state.prev || nonce = state.curr || nonce = state.next

let compute_jwk_thumbprint jwk =
  let open Yojson.Safe.Util in
  let crv = jwk |> member "crv" |> to_string in
  let kty = jwk |> member "kty" |> to_string in
  let x = jwk |> member "x" |> to_string in
  let y = jwk |> member "y" |> to_string in
  let tp =
    Printf.sprintf {|{"crv":"%s","kty":"%s","x":"%s","y":"%s"}|} crv kty x y
  in
  Digestif.SHA256.(
    digest_string tp |> to_raw_string |> Base64.encode_exn ~pad:false )

let normalize_url url =
  (* Remove query params and fragment, normalize to https://host/path *)
  let uri = Uri.of_string url in
  Uri.make ~scheme:"https"
    ~host:(Uri.host uri |> Option.get)
    ?port:(Uri.port uri) ~path:(Uri.path uri) ()
  |> Uri.to_string

let verify_signature jwt jwk alg =
  let open Yojson.Safe.Util in
  let parts = String.split_on_char '.' jwt in
  match parts with
  | [header_b64; payload_b64; sig_b64] ->
      let signing_input = header_b64 ^ "." ^ payload_b64 in
      let msg =
        Digestif.SHA256.(digest_string signing_input |> to_raw_string)
        |> Bytes.of_string
      in
      let x = jwk |> member "x" |> to_string |> Base64.decode_exn in
      let y = jwk |> member "y" |> to_string |> Base64.decode_exn in
      let pubkey =
        ( Bytes.of_string (x ^ y)
        , match alg with
          | "ES256K" ->
              (module Kleidos.K256 : Kleidos.CURVE)
          | "ES256" ->
              (module Kleidos.P256 : Kleidos.CURVE)
          | _ ->
              failwith "unsupported algorithm" )
      in
      let sig_bytes = Base64.decode_exn sig_b64 in
      let r = String.sub sig_bytes 0 32 in
      let s = String.sub sig_bytes 32 32 in
      let signature = Bytes.of_string (r ^ s) in
      Kleidos.verify ~pubkey ~msg ~signature
  | _ ->
      false

let verify_dpop_proof {nonce_state; jti_cache} ~mthd ~url ~dpop_header
    ?access_token () =
  match dpop_header with
  | None ->
      Lwt.return_error "missing dpop header"
  | Some jwt -> (
      let open Yojson.Safe.Util in
      match String.split_on_char '.' jwt with
      | [header_b64; payload_b64; _] -> (
          let header = Yojson.Safe.from_string (Base64.decode_exn header_b64) in
          let payload =
            Yojson.Safe.from_string (Base64.decode_exn payload_b64)
          in
          let typ = header |> member "typ" |> to_string in
          if typ <> "dpop+jwt" then Lwt.return_error "invalid typ in dpop proof"
          else
            let alg = header |> member "alg" |> to_string in
            if alg <> "ES256" && alg <> "ES256K" then
              Lwt.return_error "only es256 and es256k supported for dpop"
            else
              let jwk = header |> member "jwk" in
              let jti = payload |> member "jti" |> to_string in
              let htm = payload |> member "htm" |> to_string in
              let htu = payload |> member "htu" |> to_string in
              let iat = payload |> member "iat" |> to_int in
              let nonce_claim = payload |> member "nonce" |> to_string_option in
              match nonce_claim with
              (* error must be this string; see https://datatracker.ietf.org/doc/html/rfc9449#section-8 *)
              | None ->
                  Lwt.return_error "use_dpop_nonce"
              | Some n when not (verify_nonce nonce_state n) ->
                  Lwt.return_error "use_dpop_nonce"
              | Some _ ->
                  if htm <> mthd then Lwt.return_error "htm mismatch"
                  else if
                    not (String.equal (normalize_url htu) (normalize_url url))
                  then Lwt.return_error "htu mismatch"
                  else
                    let now = int_of_float (Unix.gettimeofday ()) in
                    if now - iat > 60 then Lwt.return_error "dpop proof too old"
                    else if Hashtbl.mem jti_cache jti then
                      Lwt.return_error "dpop proof replay detected"
                    else (
                      Hashtbl.add jti_cache jti (now + 3600) ;
                      if not (verify_signature jwt jwk alg) then
                        Lwt.return_error "invalid dpop signature"
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
                                |> Base64.encode_exn ~pad:false )
                            in
                            if Some expected_ath <> ath_claim then
                              Lwt.return_error "ath mismatch"
                            else Lwt.return_ok {jti; jkt; htm; htu}
                        | None ->
                            let ath_claim =
                              payload |> member "ath" |> to_string_option
                            in
                            if ath_claim <> None then
                              Lwt.return_error
                                "ath claim not allowed without access token"
                            else Lwt.return_ok {jti; jkt; htm; htu} ) )
      | _ ->
          Lwt.return_error "invalid dpop jwt" )

let create_context ?rotation_interval_ms secret =
  { nonce_state= create_nonce_state secret ?rotation_interval_ms
  ; jti_cache= Hashtbl.create 1000 }
