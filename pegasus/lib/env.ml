let data_dir = Option.value ~default:"./data" @@ Sys.getenv_opt "DATA_DIR"

let hostname = Sys.getenv "PDS_HOSTNAME"

let did =
  Option.value ~default:("did:web:" ^ hostname) @@ Sys.getenv_opt "PDS_DID"

let invite_required = Sys.getenv "INVITE_CODE_REQUIRED" = "true"

let rotation_key =
  Sys.getenv "ROTATION_KEY_MULTIBASE" |> Kleidos.parse_multikey_str

let jwt_key = Sys.getenv "JWK_MULTIBASE" |> Kleidos.parse_multikey_str

let admin_password = Sys.getenv "ADMIN_PASSWORD"

let dpop_nonce_secret =
  match Sys.getenv_opt "DPOP_NONCE_SECRET" with
  | Some sec ->
      let secret = Base64.decode_exn sec |> Bytes.of_string in
      if Bytes.length secret = 32 then secret
      else failwith "DPOP_NONCE_SECRET must be 32 bytes in base64"
  | None ->
      let secret = Mirage_crypto_rng_unix.getrandom 32 in
      Dream.warning (fun log ->
          log "DPOP_NONCE_SECRET not set; using DPOP_NONCE_SECRET=%s"
            (Base64.encode secret |> Result.get_ok) ) ;
      Bytes.of_string secret
