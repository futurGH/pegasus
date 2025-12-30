type blob = Types.blob =
  {type_: string; ref: Cid.t; mime_type: string; size: int64}

exception Xrpc_error = Types.Xrpc_error

type session = Types.session =
  { access_jwt: string
  ; refresh_jwt: string
  ; did: string
  ; handle: string
  ; pds_uri: string option
  ; email: string option
  ; email_confirmed: bool option
  ; email_auth_factor: bool option
  ; active: bool option
  ; status: string option }

type client = Client.t

type credential_manager = Credential_manager.t

let make_client = Client.make

let make_credential_manager = Credential_manager.make

let login = Credential_manager.login

let login_client = Credential_manager.login_client

let resume = Credential_manager.resume

let logout = Credential_manager.logout

let get_manager_session = Credential_manager.get_session

let on_session_update = Credential_manager.on_session_update

let on_session_expired = Credential_manager.on_session_expired

let get_session = Client.get_session

let get_service = Client.get_service

let query = Client.query

let procedure = Client.procedure

let procedure_blob = Client.procedure_blob

let query_bytes = Client.query_bytes

let procedure_bytes = Client.procedure_bytes

let session_to_yojson = Types.session_to_yojson

let session_of_yojson = Types.session_of_yojson

let blob_to_yojson = Types.blob_to_yojson

let blob_of_yojson = Types.blob_of_yojson

module Jwt = Jwt
module Http_backend = Http_backend
module Client = Client
module Credential_manager = Credential_manager
