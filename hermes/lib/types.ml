(* core types for xrpc client *)

type blob =
  {ref_: Cid.t [@key "$link"]; mime_type: string [@key "mimeType"]; size: int64}
[@@deriving yojson]

type xrpc_error_payload = {error: string; message: string option [@default None]}
[@@deriving yojson {strict= false}]

exception Xrpc_error of {status: int; error: string; message: string option}

type session =
  { access_jwt: string [@key "accessJwt"]
  ; refresh_jwt: string [@key "refreshJwt"]
  ; did: string
  ; handle: string
  ; pds_uri: string option [@key "pdsUri"] [@default None]
  ; email: string option [@default None]
  ; email_confirmed: bool option [@key "emailConfirmed"] [@default None]
  ; email_auth_factor: bool option [@key "emailAuthFactor"] [@default None]
  ; active: bool option [@default None]
  ; status: string option [@default None] }
[@@deriving yojson {strict= false}]

type login_request =
  { identifier: string
  ; password: string
  ; auth_factor_token: string option [@key "authFactorToken"] [@default None] }
[@@deriving yojson]

type refresh_request = unit [@@deriving yojson]

let raise_xrpc_error ~status (payload : xrpc_error_payload) =
  raise (Xrpc_error {status; error= payload.error; message= payload.message})

let raise_xrpc_error_raw ~status ~error ?message () =
  raise (Xrpc_error {status; error; message})
