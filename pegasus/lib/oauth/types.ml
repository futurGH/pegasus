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
  { client_id: string option [@default None]
  ; client_name: string option [@default None]
  ; client_uri: string option [@default None]
  ; policy_uri: string option [@default None]
  ; tos_uri: string option [@default None]
  ; logo_uri: string option [@default None]
  ; redirect_uris: string list
  ; grant_types: string list [@default ["authorization_code"]]
  ; response_types: string list [@default ["code"]]
  ; scope: string option [@default None]
  ; token_endpoint_auth_method: string [@default "client_secret_basic"]
  ; token_endpoint_auth_signing_alg: string option [@default None]
  ; userinfo_signed_response_alg: string option [@default None]
  ; userinfo_encrypted_response_alg: string option [@default None]
  ; application_type: string [@default "web"]
  ; subject_type: string [@default "public"]
  ; request_object_signing_alg: string option [@default None]
  ; id_token_signed_response_alg: string option [@default None]
  ; authorization_signed_response_alg: string [@default "RS256"]
  ; authorization_encrypted_response_enc: string option [@default None]
  ; authorization_encrypted_response_alg: string option [@default None]
  ; authorization_details_types: string list option [@default None]
  ; dpop_bound_access_tokens: bool option [@default None]
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
  ; authorized_ip: string option [@default None]
  ; authorized_user_agent: string option [@default None]
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
