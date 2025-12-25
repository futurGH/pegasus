let getenv name =
  try Sys.getenv name
  with Not_found -> failwith ("Missing environment variable " ^ name)

let getenv_opt name ~default =
  match Sys.getenv_opt name with
  | Some value when value <> "" ->
      value
  | _ ->
      default

let log_level =
  match getenv_opt "PDS_LOG_LEVEL" ~default:"info" with
  | "debug" ->
      `Debug
  | "warn" | "warning" ->
      `Warning
  | "error" ->
      `Error
  | _ ->
      `Info

let data_dir = getenv_opt "PDS_DATA_DIR" ~default:"./data"

let hostname = getenv "PDS_HOSTNAME"

let host_endpoint = "https://" ^ hostname

let did = getenv_opt "PDS_DID" ~default:("did:web:" ^ hostname)

let invite_required =
  getenv_opt "PDS_INVITE_CODE_REQUIRED" ~default:"true" = "true"

let rotation_key =
  getenv "PDS_ROTATION_KEY_MULTIBASE" |> Kleidos.parse_multikey_str

let jwt_key = getenv "PDS_JWK_MULTIBASE" |> Kleidos.parse_multikey_str

let jwt_pubkey = Kleidos.derive_pubkey jwt_key

let admin_password = getenv "PDS_ADMIN_PASSWORD"

let crawlers =
  getenv_opt "PDS_CRAWLERS" ~default:"https://bsky.network"
  |> String.split_on_char ','
  |> List.map (fun u ->
      match Uri.of_string @@ String.trim u with
      | uri when Uri.host uri <> None ->
          uri
      | _ ->
          Uri.make ~scheme:"https" ~host:u () )

let favicon_url = getenv_opt "PDS_FAVICON_URL" ~default:"/public/favicon.ico"

let dpop_nonce_secret =
  match getenv_opt "PDS_DPOP_NONCE_SECRET" ~default:"" with
  | "" ->
      let secret = Mirage_crypto_rng_unix.getrandom 32 in
      Dream.warning (fun log ->
          log "PDS_DPOP_NONCE_SECRET not set; using PDS_DPOP_NONCE_SECRET=%s"
            ( Base64.(encode ~alphabet:uri_safe_alphabet ~pad:false) secret
            |> Result.get_ok ) ) ;
      Bytes.of_string secret
  | sec ->
      let secret =
        Base64.(decode_exn ~alphabet:uri_safe_alphabet ~pad:false) sec
        |> Bytes.of_string
      in
      if Bytes.length secret = 32 then secret
      else failwith "PDS_DPOP_NONCE_SECRET must be 32 bytes in base64uri"

let smtp_config, smtp_sender =
  begin
    let with_starttls =
      getenv_opt "PDS_SMTP_STARTTLS" ~default:"false" = "true"
    in
    match
      ( getenv_opt "PDS_SMTP_AUTH_URI" ~default:""
      , getenv_opt "PDS_SMTP_SENDER" ~default:"" )
    with
    | "", "" ->
        (None, None)
    | _uri, "" ->
        failwith
          "PDS_SMTP_SENDER must be set alongside PDS_SMTP_AUTH_URI; it should \
           look like `e@mail.com` or `Name <e@mail.com>`"
    | "", _uri ->
        failwith "PDS_SMTP_AUTH_URI must be set alongside PDS_SMTP_SENDER"
    | uri, sender -> (
        let uri = Uri.of_string uri in
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
                "PDS_SMTP_SENDER should be a valid mailbox, e.g. `e@mail.com` \
                 or `Name <e@mail.com>`" )
        | _ ->
            failwith
              "PDS_SMTP_AUTH_URI must be a valid smtp:// or smtps:// URI with \
               username, password, and hostname" )
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
      getenv_opt "PDS_S3_BLOBS_ENABLED" ~default:"false" = "true"
    in
    let backups_enabled =
      getenv_opt "PDS_S3_BACKUPS_ENABLED" ~default:"false" = "true"
    in
    let backup_interval_s =
      getenv_opt "PDS_S3_BACKUP_INTERVAL_S"
        ~default:(string_of_float default_backup_interval)
      |> float_of_string
    in
    let endpoint = Sys.getenv_opt "PDS_S3_ENDPOINT" in
    match (blobs_enabled, backups_enabled) with
    | true, _ | _, true -> (
      match
        ( Sys.getenv_opt "PDS_S3_REGION"
        , Sys.getenv_opt "PDS_S3_BUCKET"
        , Sys.getenv_opt "PDS_S3_ACCESS_KEY"
        , Sys.getenv_opt "PDS_S3_SECRET_KEY"
        , Sys.getenv_opt "PDS_S3_CDN_URL" )
      with
      | Some region, Some bucket, Some access_key, Some secret_key, cdn_url ->
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
          let credentials_obj =
            Aws_s3.Credentials.make ~access_key ~secret_key ()
          in
          Some
            { blobs_enabled
            ; backups_enabled
            ; backup_interval_s
            ; endpoint
            ; region
            ; bucket
            ; access_key
            ; secret_key
            ; cdn_url
            ; credentials_obj
            ; endpoint_obj }
      | _ ->
          failwith
            "S3 backups require the following environment variables: \
             PDS_S3_REGION, PDS_S3_BUCKET, PDS_S3_ACCESS_KEY, \
             PDS_S3_SECRET_KEY" )
    | _ ->
        None
  end
