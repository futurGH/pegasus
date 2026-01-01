module Defaults = struct
  let service_token_exp = 60 * 5 (* 5 minutes *)

  let access_token_exp = 60 * 60 * 3 (* 3 hours *)

  let refresh_token_exp = 60 * 60 * 24 * 7 (* 7 days *)
end

type service_jwt = {iss: string; aud: string; lxm: string; exp: int}
[@@deriving yojson]

type symmetric_jwt =
  {scope: string; aud: string; sub: string; iat: int; exp: int; jti: string}
[@@deriving yojson]

let b64_encode str =
  Base64.encode_string ~pad:false ~alphabet:Base64.uri_safe_alphabet str

let b64_decode str =
  match Base64.decode ~pad:false ~alphabet:Base64.uri_safe_alphabet str with
  | Ok s ->
      s
  | Error (`Msg e) ->
      failwith e

let extract_signature_components signature =
  if Bytes.length signature <> 64 then failwith "expected 64 byte jwt signature"
  else
    let r = Bytes.sub signature 0 32 in
    let s = Bytes.sub signature 32 32 in
    (r, s)

let sign_jwt ?(typ = "JWT") ~signing_key payload =
  let _, (module Curve : Kleidos.CURVE) = signing_key in
  let alg =
    match Curve.name with
    | "K256" ->
        "ES256K"
    | "P256" ->
        "ES256"
    | _ ->
        failwith "invalid curve"
  in
  let crv =
    match Curve.name with
    | "K256" ->
        "secp256k1"
    | "P256" ->
        "P-256"
    | _ ->
        failwith "invalid curve"
  in
  let header_json =
    `Assoc [("alg", `String alg); ("crv", `String crv); ("typ", `String typ)]
  in
  let encoded_header = header_json |> Yojson.Safe.to_string |> b64_encode in
  let encoded_payload = payload |> Yojson.Safe.to_string |> b64_encode in
  let signing_input = encoded_header ^ "." ^ encoded_payload in
  let signature =
    Kleidos.sign ~privkey:signing_key ~msg:(Bytes.of_string signing_input)
  in
  let encoded_signature = b64_encode (Bytes.to_string signature) in
  signing_input ^ "." ^ encoded_signature

let decode_jwt jwt =
  match String.split_on_char '.' jwt with
  | [header_b64; payload_b64; _] -> (
    try
      let header = Yojson.Safe.from_string (b64_decode header_b64) in
      let payload = Yojson.Safe.from_string (b64_decode payload_b64) in
      Ok (header, payload)
    with _ -> Error "invalid jwt" )
  | _ ->
      Error "invalid jwt format"

let verify_jwt jwt ~pubkey =
  match String.split_on_char '.' jwt with
  | [header_b64; payload_b64; signature_b64] ->
      let signature = Bytes.of_string (b64_decode signature_b64) in
      let signing_input = header_b64 ^ "." ^ payload_b64 in
      let verified =
        Kleidos.verify ~pubkey ~msg:(Bytes.of_string signing_input) ~signature
      in
      if verified then decode_jwt jwt
      else Error "jwt signature verification failed"
  | _ ->
      Error "invalid jwt format"

let generate_jwt did =
  let now_s = int_of_float (Unix.gettimeofday ()) in
  let access_exp = now_s + Defaults.access_token_exp in
  let refresh_exp = now_s + Defaults.refresh_token_exp in
  let jti =
    Uuidm.v4_gen (Random.State.make_self_init ()) () |> Uuidm.to_string
  in
  let access_payload =
    symmetric_jwt_to_yojson
      { scope= "com.atproto.access"
      ; aud= Env.did
      ; sub= did
      ; iat= now_s
      ; exp= access_exp
      ; jti }
  in
  let refresh_payload =
    symmetric_jwt_to_yojson
      { scope= "com.atproto.refresh"
      ; aud= Env.did
      ; sub= did
      ; iat= now_s
      ; exp= refresh_exp
      ; jti }
  in
  let access = sign_jwt access_payload ~signing_key:Env.jwt_key in
  let refresh = sign_jwt refresh_payload ~signing_key:Env.jwt_key in
  (access, refresh)

let generate_service_jwt ~did ~aud ~lxm ~signing_key =
  let now_s = int_of_float (Unix.gettimeofday ()) in
  let exp = now_s + Defaults.service_token_exp in
  let payload = service_jwt_to_yojson {iss= did; aud; lxm; exp} in
  sign_jwt payload ~signing_key

type verify_jwt_error =
  | AuthRequired of string
  | ExpiredToken of string
  | InvalidToken of string
  | InternalError of string

(* if no did is provided, iss did will be assumed to be correct *)
let verify_service_jwt ~nsid ?did ~(verify_sig : string -> string -> 'a) token =
  match decode_jwt token with
  | Error e ->
      Lwt.return_error @@ AuthRequired e
  | Ok (_header, payload) -> (
    try
      let open Yojson.Safe.Util in
      let iss = payload |> member "iss" |> to_string in
      let aud = payload |> member "aud" |> to_string in
      let exp = payload |> member "exp" |> to_int in
      let lxm = payload |> member "lxm" |> to_string_option in
      let now = int_of_float (Unix.gettimeofday ()) in
      if exp < now then Lwt.return_error @@ ExpiredToken "token expired"
      else if aud <> Env.did then
        Lwt.return_error
        @@ InvalidToken "jwt audience does not match service did"
      else
        let iss_did =
          match String.split_on_char '#' iss with did :: _ -> did | [] -> iss
        in
        if did <> None && Some iss_did <> did then
          Lwt.return_error @@ InvalidToken "jwt issuer does not match did"
        else
          match lxm with
          | Some l when l <> nsid && l <> "*" ->
              Lwt.return_error
              @@ InvalidToken ("jwt lxm " ^ l ^ " does not match " ^ nsid)
          | _ -> (
              let did = Option.value did ~default:iss_did in
              match%lwt Id_resolver.Did.resolve did with
              | Error e ->
                  Log.debug (fun log ->
                      log "failed to resolve did %s: %s" did e ) ;
                  Lwt.return_error
                  @@ InternalError "could not resolve jwt issuer did"
              | Ok did_doc -> (
                match
                  Id_resolver.Did.Document.get_verification_key did_doc
                    "#atproto"
                with
                | None ->
                    Lwt.return_error
                    @@ InternalError "missing or bad key in issuer did doc"
                | Some pubkey_multibase -> (
                  match%lwt verify_sig did pubkey_multibase with
                  | Ok creds ->
                      Lwt.return_ok creds
                  | Error _ -> (
                    (* try again, skipping cache in case of key rotation *)
                    match%lwt
                      Id_resolver.Did.resolve ~skip_cache:true did
                    with
                    | Error _ ->
                        Lwt.return_error
                        @@ InvalidToken
                             "jwt signature does not match jwt issuer"
                    | Ok fresh_doc -> (
                      match
                        Id_resolver.Did.Document.get_verification_key fresh_doc
                          "#atproto"
                      with
                      | None ->
                          Lwt.return_error
                          @@ InvalidToken
                               "jwt signature does not match jwt issuer"
                      | Some fresh_pubkey_multibase
                        when fresh_pubkey_multibase = pubkey_multibase ->
                          Lwt.return_error
                          @@ InvalidToken
                               "jwt signature does not match jwt issuer"
                      | Some fresh_pubkey_multibase -> (
                        match%lwt verify_sig did fresh_pubkey_multibase with
                        | Ok creds ->
                            Lwt.return_ok creds
                        | Error e ->
                            Lwt.return_error @@ InternalError e ) ) ) ) ) )
    with _ -> Lwt.return_error @@ InvalidToken "malformed service jwt" )
