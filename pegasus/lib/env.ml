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

type s3_config =
  { blobs_enabled: bool
  ; backups_enabled: bool
  ; backup_interval_s: float
  ; endpoint: string option
  ; region: string
  ; bucket: string
  ; access_key: string
  ; secret_key: string
  ; cdn_url: string option
  ; credentials_obj: Aws_s3.Credentials.t
  ; endpoint_obj: Aws_s3.Region.endpoint }

let s3_config =
  begin
    let default_backup_interval = 3600.0 in
    let blobs_enabled =
      Sys.getenv_opt "S3_BLOBS_ENABLED" |> Option.map (( = ) "true")
    in
    let backups_enabled =
      Sys.getenv_opt "S3_BACKUPS_ENABLED" |> Option.map (( = ) "true")
    in
    let backup_interval_s =
      Sys.getenv_opt "S3_BACKUP_INTERVAL_S"
      |> Option.map float_of_string_opt
      |> Option.join
    in
    let endpoint = Sys.getenv_opt "S3_ENDPOINT" in
    match (blobs_enabled, backups_enabled) with
    | Some true, _ | _, Some true -> (
      match
        ( Sys.getenv_opt "S3_REGION"
        , Sys.getenv_opt "S3_BUCKET"
        , Sys.getenv_opt "S3_ACCESS_KEY"
        , Sys.getenv_opt "S3_SECRET_KEY" )
      with
      | Some region, Some bucket, Some access_key, Some secret_key ->
          let region_obj =
            match endpoint with
            | Some host ->
                Aws_s3.Region.vendor ~region_name:region ~host ()
            | None ->
                Aws_s3.Region.of_string region
          in
          let endpoint_obj =
            Aws_s3.Region.endpoint ~inet:`V4 ~scheme:`Https region_obj
          in
          Some
            { blobs_enabled= Option.value ~default:false blobs_enabled
            ; backups_enabled= Option.value ~default:false backups_enabled
            ; backup_interval_s=
                Option.value ~default:default_backup_interval backup_interval_s
            ; endpoint
            ; region
            ; bucket
            ; access_key
            ; secret_key
            ; cdn_url= Sys.getenv_opt "S3_CDN_URL"
            ; credentials_obj=
                Aws_s3.Credentials.make ~access_key ~secret_key ()
            ; endpoint_obj }
      | _ ->
          failwith
            "S3 backups require the following environment variables: \
             S3_REGION, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY" )
    | _ ->
        None
  end
