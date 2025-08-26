type env = {database_dir: string; hostname: string; invite_required: bool}

let load () : env =
  let database_dir =
    Option.value ~default:"./db" @@ Sys.getenv_opt "DATABASE_DIR"
  in
  let hostname = Sys.getenv "PDS_HOSTNAME" in
  let invite_required = Sys.getenv "INVITE_CODE_REQUIRED" = "true" in
  {database_dir; hostname; invite_required}
