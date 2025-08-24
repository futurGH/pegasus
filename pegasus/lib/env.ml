type env = {hostname: string; invite_required: bool}

let load () =
  let hostname = Sys.getenv "PDS_HOSTNAME" in
  let invite_required = Sys.getenv "INVITE_CODE_REQUIRED" = "true" in
  {hostname; invite_required}
