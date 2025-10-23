type par_request =
  { client_id: string
  ; response_type: string
  ; redirect_uri: string
  ; scope: string
  ; state: string
  ; code_challenge: string
  ; code_challenge_method: string
  ; login_hint: string option
  ; dpop_jkt: string option
  ; client_assertion_type: string option
  ; client_assertion: string option }
[@@deriving yojson {strict= false}]

type token_request =
  { grant_type: string
  ; code: string option
  ; redirect_uri: string option
  ; code_verifier: string option
  ; refresh_token: string option
  ; client_id: string
  ; client_assertion_type: string option
  ; client_assertion: string option }
[@@deriving yojson {strict= false}]

type client_metadata =
  { client_id: string
  ; client_name: string option
  ; client_uri: string
  ; redirect_uris: string list
  ; grant_types: string list
  ; response_types: string list
  ; scope: string
  ; token_endpoint_auth_method: string
  ; token_endpoint_auth_signing_alg: string option
  ; application_type: string
  ; dpop_bound_access_tokens: bool
  ; jwks_uri: string option
  ; jwks: Yojson.Safe.t option }
[@@deriving yojson {strict= false}]

type dpop_proof = {jti: string; jkt: string; htm: string; htu: string}
[@@deriving yojson {strict= false}]

type oauth_request =
  { request_id: string
  ; client_id: string
  ; request_data: string
  ; dpop_jkt: string option
  ; expires_at: int
  ; created_at: int }
[@@deriving yojson {strict= false}]

type oauth_code =
  { code: string
  ; request_id: string
  ; authorized_by: string option
  ; authorized_at: int option
  ; expires_at: int
  ; used: bool }
[@@deriving yojson {strict= false}]

type oauth_token =
  { id: int
  ; token_id: string
  ; refresh_token: string
  ; client_id: string
  ; did: string
  ; dpop_jkt: string
  ; scope: string
  ; created_at: int
  ; expires_at: int
  ; last_refreshed_at: int }
[@@deriving yojson {strict= false}]
