let getenv name =
  try Sys.getenv name
  with Not_found -> failwith ("Missing environment variable " ^ name)

let data_dir = Option.value ~default:"./data" @@ Sys.getenv_opt "DATA_DIR"

let hostname = getenv "PDS_HOSTNAME"

let did =
  Option.value ~default:("did:web:" ^ hostname) @@ Sys.getenv_opt "PDS_DID"

let invite_required = getenv "INVITE_CODE_REQUIRED" = "true"

let rotation_key = getenv "ROTATION_KEY_MULTIBASE" |> Kleidos.parse_multikey_str

let jwt_key = getenv "JWK_MULTIBASE" |> Kleidos.parse_multikey_str

let jwt_pubkey = Kleidos.derive_pubkey jwt_key

let admin_password = getenv "ADMIN_PASSWORD"

let dpop_nonce_secret =
  match Sys.getenv_opt "DPOP_NONCE_SECRET" with
  | Some sec ->
      let secret =
        Base64.(decode_exn ~alphabet:uri_safe_alphabet ~pad:false) sec
        |> Bytes.of_string
      in
      if Bytes.length secret = 32 then secret
      else failwith "DPOP_NONCE_SECRET must be 32 bytes in base64uri"
  | None ->
      let secret = Mirage_crypto_rng_unix.getrandom 32 in
      Dream.warning (fun log ->
          log "DPOP_NONCE_SECRET not set; using DPOP_NONCE_SECRET=%s"
            ( Base64.(encode ~alphabet:uri_safe_alphabet ~pad:false) secret
            |> Result.get_ok ) ) ;
      Bytes.of_string secret
