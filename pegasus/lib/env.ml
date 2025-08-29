let database_dir = Option.value ~default:"./db" @@ Sys.getenv_opt "DATABASE_DIR"

let hostname = Sys.getenv "PDS_HOSTNAME"

let invite_required = Sys.getenv "INVITE_CODE_REQUIRED" = "true"

let rotation_key =
  Sys.getenv "ROTATION_KEY_MULTIBASE" |> Kleidos.parse_multikey_str
