type access_token =
  {token: string; token_id: string; expires_in: int; expires_at: int}

type refresh_token = {token: string; token_id: string; expires_at: int}

type refresh_token_error = Expired | Invalid of string

let refresh_token_scope = "com.atproto.oauth.refresh"

let next_token_id prefix =
  prefix ^ Uuidm.to_string (Uuidm.v4_gen (Random.State.make_self_init ()) ())

let access_expires_in_s = Constants.access_token_expiry_ms / 1000

let refresh_expires_in_s = Constants.refresh_token_expiry_ms / 1000

let session_expires_at_ms created_at_ms =
  created_at_ms + Constants.session_expiry_ms

let generate_access ?(now = Util.Time.now_s ()) ?token_id ~did ~scope ~jkt () =
  let token_id = Option.value token_id ~default:(next_token_id "tok-") in
  let exp = now + access_expires_in_s in
  let claims =
    `Assoc
      [ ("jti", `String token_id)
      ; ("sub", `String did)
      ; ("iat", `Int now)
      ; ("exp", `Int exp)
      ; ("scope", `String scope)
      ; ("aud", `String Env.host_endpoint)
      ; ("cnf", `Assoc [("jkt", `String jkt)]) ]
  in
  { token= Jwt.sign_jwt claims ~typ:"at+jwt" ~signing_key:Env.jwt_key
  ; token_id
  ; expires_in= access_expires_in_s
  ; expires_at= exp * 1000 }

let generate_refresh ?(now = Util.Time.now_s ()) ?token_id ~did () =
  let token_id = Option.value token_id ~default:(next_token_id "ref-") in
  let exp = now + refresh_expires_in_s in
  let claims =
    Jwt.symmetric_jwt_to_yojson
      { scope= refresh_token_scope
      ; aud= Env.host_endpoint
      ; sub= did
      ; iat= now
      ; exp
      ; jti= token_id }
  in
  { token= Jwt.sign_jwt claims ~typ:"refresh+jwt" ~signing_key:Env.jwt_key
  ; token_id
  ; expires_at= exp * 1000 }

let generate_tokens ?(now = Util.Time.now_s ()) ?token_id ~did ~scope ~jkt () =
  ( generate_access ~now ?token_id ~did ~scope ~jkt ()
  , generate_refresh ~now ?token_id ~did () )

let verify_refresh_token ?(now = Util.Time.now_s ()) token =
  match Jwt.verify_jwt token ~pubkey:Env.jwt_pubkey with
  | Error e ->
      Error (Invalid e)
  | Ok (header, payload) -> (
      let open Yojson.Safe.Util in
      match header |> member "typ" |> to_string_option with
      | Some "refresh+jwt" -> (
        match Jwt.symmetric_jwt_of_yojson payload with
        | Error e ->
            Error (Invalid e)
        | Ok claims ->
            if claims.scope <> refresh_token_scope then
              Error (Invalid "invalid scope")
            else if claims.aud <> Env.host_endpoint then
              Error (Invalid "invalid audience")
            else if claims.sub = "" then Error (Invalid "missing sub")
            else if claims.jti = "" then Error (Invalid "missing jti")
            else if now < claims.iat then
              Error (Invalid "token issued in the future")
            else if now >= claims.exp then Error Expired
            else Ok claims )
      | Some _ ->
          Error (Invalid "invalid token type")
      | None ->
          Error (Invalid "missing token type") )
