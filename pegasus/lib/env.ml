let data_dir = Option.value ~default:"./data" @@ Sys.getenv_opt "DATA_DIR"

let hostname = Sys.getenv "PDS_HOSTNAME"

let did =
  Option.value ~default:("did:web:" ^ hostname) @@ Sys.getenv_opt "PDS_DID"

let invite_required = Sys.getenv "INVITE_CODE_REQUIRED" = "true"

let rotation_key =
  Sys.getenv "ROTATION_KEY_MULTIBASE" |> Kleidos.parse_multikey_str

let admin_password = Sys.getenv "ADMIN_PASSWORD"

let jwt_secret = Sys.getenv "JWT_SECRET"
