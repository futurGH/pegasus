let getenv name =
  try Sys.getenv name
  with Not_found -> failwith ("Missing environment variable " ^ name)

let data_dir = Option.value ~default:"./data" @@ Sys.getenv_opt "DATA_DIR"

let hostname = getenv "PDS_HOSTNAME"

let host_endpoint = "https://" ^ hostname

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

let smtp_config, smtp_sender =
  begin
    let with_starttls =
      Option.value ~default:"false" @@ Sys.getenv_opt "SMTP_STARTTLS" = "true"
    in
    match
      ( Option.map Uri.of_string (Sys.getenv_opt "SMTP_AUTH_URI")
      , Sys.getenv_opt "SMTP_SENDER" )
    with
    | Some uri, Some sender -> (
      match
        ( Uri.scheme uri
        , Uri.user uri
        , Uri.password uri
        , Uri.host uri
        , Uri.port uri )
      with
      | Some scheme, Some username, Some password, Some hostname, port
        when scheme = "smtp" || scheme = "smtps" -> (
        match Emile.of_string sender with
        | Ok _ ->
            ( Some
                Letters.Config.(
                  create ~username ~password ~hostname ~with_starttls ()
                  |> set_port port )
            , Some sender )
        | Error _ ->
            failwith
              "SMTP_SENDER should be a valid mailbox, e.g. `e@mail.com` or \
               `Name <e@mail.com>`" )
      | _ ->
          failwith
            "SMTP_AUTH_URI must be a valid smtp:// or smtps:// URI with \
             username, password, and hostname" )
    | Some _, None ->
        failwith
          "SMTP_SENDER must be set alongside SMTP_AUTH_URI; it should look \
           like `e@mail.com` or `Name <e@mail.com>`"
    | None, Some _ ->
        failwith "SMTP_AUTH_URI must be set alongside SMTP_SENDER"
    | None, None ->
        (None, None)
  end
