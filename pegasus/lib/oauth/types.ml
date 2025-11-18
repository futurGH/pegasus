type par_request =
  { client_id: string
  ; response_type: string
  ; response_mode: string option [@default None]
  ; redirect_uri: string
  ; scope: string
  ; state: string
  ; code_challenge: string
  ; code_challenge_method: string
  ; login_hint: string option [@default None]
  ; dpop_jkt: string option [@default None]
  ; client_assertion_type: string option [@default None]
  ; client_assertion: string option [@default None] }
[@@deriving yojson {strict= false}]

type token_request =
  { grant_type: string
  ; code: string option [@default None]
  ; redirect_uri: string option [@default None]
  ; code_verifier: string option [@default None]
  ; refresh_token: string option [@default None]
  ; client_id: string
  ; client_assertion_type: string option [@default None]
  ; client_assertion: string option [@default None] }
[@@deriving yojson {strict= false}]

type client_metadata =
  { client_id: string
  ; client_name: string option [@default None]
  ; client_uri: string
  ; redirect_uris: string list
  ; grant_types: string list
  ; response_types: string list
  ; scope: string
  ; token_endpoint_auth_method: string
  ; token_endpoint_auth_signing_alg: string option [@default None]
  ; application_type: string
  ; dpop_bound_access_tokens: bool
  ; jwks_uri: string option [@default None]
  ; jwks: Yojson.Safe.t option [@default None] }
[@@deriving yojson {strict= false}]

type dpop_proof = {jti: string; jkt: string; htm: string; htu: string}
[@@deriving yojson {strict= false}]

type oauth_request =
  { request_id: string
  ; client_id: string
  ; request_data: string
  ; dpop_jkt: string option [@default None]
  ; expires_at: int
  ; created_at: int }
[@@deriving yojson {strict= false}]

type oauth_code =
  { code: string
  ; request_id: string
  ; authorized_by: string option [@default None]
  ; authorized_at: int option [@default None]
  ; expires_at: int
  ; used: bool }
[@@deriving yojson {strict= false}]

type oauth_token =
  { refresh_token: string
  ; client_id: string
  ; did: string
  ; dpop_jkt: string
  ; scope: string
  ; created_at: int
  ; last_refreshed_at: int
  ; expires_at: int
  ; last_ip: string
  ; last_user_agent: string option [@default None] }
[@@deriving yojson {strict= false}]
