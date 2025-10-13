type t = (module Rapper_helper.CONNECTION)

type session_info =
  { handle: string
  ; did: string
  ; email: string
  ; email_confirmed: bool [@key "emailConfirmed"]
  ; email_auth_factor: bool [@key "emailAuthFactor"]
  ; active: bool option
  ; status: string option }
[@@deriving yojson {strict= false}]

type credentials =
  | Unauthenticated
  | Admin
  | Access of {did: string}
  | Refresh of {did: string; jti: string}

let dpop_nonce_state = ref (Oauth.Dpop.create_nonce_state Env.dpop_nonce_secret)

let verify_bearer_jwt t token expected_scope =
  match Jwt.verify_jwt token Env.jwt_key with
  | Error err ->
      Lwt.return_error err
  | Ok (_, payload) -> (
    try
      let now_s = int_of_float (Unix.gettimeofday ()) in
      let jwt = Jwt.symmetric_jwt_of_yojson payload |> Result.get_ok in
      if jwt.aud <> Env.did then Lwt.return_error "invalid aud"
      else if jwt.sub = "" then Lwt.return_error "missing sub"
      else if now_s < jwt.iat then Lwt.return_error "token issued in the future"
      else if now_s > jwt.exp then Lwt.return_error "expired token"
      else if jwt.scope <> expected_scope then Lwt.return_error "invalid scope"
      else if jwt.jti = "" then Lwt.return_error "missing jti"
      else
        let%lwt revoked_at =
          Data_store.is_token_revoked t ~did:jwt.sub ~jti:jwt.jti
        in
        if revoked_at <> None then Lwt.return_error "token revoked"
        else Lwt.return_ok jwt
    with _ -> Lwt.return_error "invalid token format" )

let verify_auth ?(refresh = false) credentials did =
  match credentials with
  | Admin ->
      true
  | Access {did= creds} when creds = did ->
      true
  | Refresh {did= creds; _} when creds = did && refresh ->
      true
  | _ ->
      false

let get_authed_did_exn = function
  | Access {did} ->
      did
  | Refresh {did; _} ->
      did
  | _ ->
      Errors.auth_required "Invalid authorization header"

let get_session_info identifier db =
  let%lwt actor =
    match%lwt Data_store.get_actor_by_identifier identifier db with
    | Some actor ->
        Lwt.return actor
    | None ->
        failwith "actor not found"
  in
  let active, status =
    match actor.deactivated_at with
    | None ->
        (Some true, None)
    | Some _ ->
        (Some false, Some "deactivated")
  in
  Lwt.return
    { did= actor.did
    ; handle= actor.handle
    ; email= actor.email
    ; email_confirmed= true
    ; email_auth_factor= true
    ; active
    ; status }

module Verifiers = struct
  open struct
    let parse_header req expected_type =
      match Dream.header req "authorization" with
      | Some header -> (
        match String.split_on_char ' ' header with
        | [typ; token]
          when String.uppercase_ascii typ = String.uppercase_ascii expected_type
          ->
            Ok token
        | _ ->
            Error "invalid authorization header" )
      | None ->
          Error "missing authorization header"

    let parse_basic req =
      match parse_header req "Basic" with
      | Ok token -> (
        match Base64.decode token with
        | Ok decoded -> (
          match Str.bounded_split (Str.regexp_string ":") decoded 2 with
          | [username; password] ->
              Ok (username, password)
          | _ ->
              Error "invalid basic authorization header" )
        | Error _ ->
            Error "invalid basic authorization header" )
      | Error _ ->
          Error "invalid basic authorization header"

    let parse_bearer req = parse_header req "Bearer"
  end

  type ctx = {req: Dream.request; db: Data_store.t}

  type verifier = ctx -> (credentials, exn) Lwt_result.t

  let unauthenticated : verifier =
   fun {req; _} ->
    match Dream.header req "authorization" with
    | Some _ ->
        Lwt.return_error @@ Errors.auth_required "Invalid authorization header"
    | None ->
        Lwt.return_ok Unauthenticated

  let admin : verifier =
   fun {req; _} ->
    match parse_basic req with
    | Ok (username, password) -> (
      match (username, password) with
      | "admin", p when p = Env.admin_password ->
          Lwt.return_ok Admin
      | _ ->
          Lwt.return_error @@ Errors.auth_required "Invalid credentials" )
    | Error _ ->
        Lwt.return_error @@ Errors.auth_required "Invalid authorization header"

  let bearer : verifier =
   fun {req; db} ->
    match parse_bearer req with
    | Ok jwt -> (
        match%lwt verify_bearer_jwt db jwt "com.atproto.access" with
        | Ok {sub= did; _} -> (
            match%lwt Data_store.get_actor_by_identifier did db with
            | Some {deactivated_at= None; _} ->
                Lwt.return_ok (Access {did})
            | Some {deactivated_at= Some _; _} ->
                Lwt.return_error
                @@ Errors.auth_required ~name:"AccountDeactivated"
                     "Account is deactivated"
            | None ->
                Lwt.return_error @@ Errors.auth_required "Invalid credentials" )
        | Error _ ->
            Lwt.return_error @@ Errors.auth_required "Invalid credentials" )
    | Error _ ->
        Lwt.return_error @@ Errors.auth_required "Invalid authorization header"

  let oauth : verifier =
   fun {req; db} ->
    match Dream.header req "Authorization" with
    | None ->
        Lwt.return_error @@ Errors.auth_required "missing authorization header"
    | Some auth ->
        if String.starts_with ~prefix:"DPoP " auth then
          let token = String.sub auth 5 (String.length auth - 5) in
          let dpop_header = Dream.header req "DPoP" in
          let full_url = "https://" ^ Env.hostname ^ Dream.target req in
          let%lwt dpop_result =
            Oauth.Dpop.verify_dpop_proof ~nonce_state:!dpop_nonce_state
              ~mthd:(Dream.method_to_string @@ Dream.method_ req)
              ~url:full_url ~dpop_header ~access_token:token ()
          in
          match dpop_result with
          | Error e ->
              Lwt.return_error @@ Errors.auth_required ("dpop: " ^ e)
          | Ok proof -> (
            match Jwt.decode_jwt token with
            | Error e ->
                Lwt.return_error @@ Errors.auth_required e
            | Ok (_header, claims) -> (
                let open Yojson.Safe.Util in
                try
                  let did = claims |> member "sub" |> to_string in
                  let exp = claims |> member "exp" |> to_int in
                  let jkt_claim =
                    claims |> member "cnf" |> member "jkt" |> to_string
                  in
                  if jkt_claim <> proof.jkt then
                    Lwt.return_error @@ Errors.auth_required "dpop key mismatch"
                  else
                    let now = int_of_float (Unix.gettimeofday ()) in
                    if exp < now then
                      Lwt.return_error @@ Errors.auth_required "token expired"
                    else
                      match Jwt.verify_jwt token Env.jwt_key with
                      | Error e ->
                          Lwt.return_error @@ Errors.auth_required e
                      | Ok _ ->
                          Lwt.return_ok (Access {did})
                with _ ->
                  Lwt.return_error
                  @@ Errors.auth_required "malformed JWT claims" ) )
        else bearer {req; db}

  let refresh : verifier =
   fun {req; db} ->
    match parse_bearer req with
    | Ok jwt -> (
        match%lwt verify_bearer_jwt db jwt "com.atproto.refresh" with
        | Ok {sub= did; jti; _} -> (
            match%lwt Data_store.get_actor_by_identifier did db with
            | Some {deactivated_at= None; _} ->
                Lwt.return_ok (Refresh {did; jti})
            | Some {deactivated_at= Some _; _} ->
                Lwt.return_error
                @@ Errors.auth_required ~name:"AccountDeactivated"
                     "Account is deactivated"
            | None ->
                Lwt.return_error @@ Errors.auth_required "Invalid credentials" )
        | Error "" | Error _ ->
            Lwt.return_error @@ Errors.auth_required "Invalid credentials" )
    | Error _ ->
        Lwt.return_error @@ Errors.auth_required "Invalid authorization header"

  let authorization : verifier =
   fun ctx ->
    match
      Dream.header ctx.req "Authorization"
      |> Option.map @@ String.split_on_char ' '
    with
    | Some ("Basic" :: _) ->
        admin ctx
    | Some ("Bearer" :: _) ->
        bearer ctx
    | Some ("DPoP" :: _) ->
        oauth ctx
    | _ ->
        Lwt.return_error
        @@ Errors.auth_required ~name:"InvalidToken"
             "Unexpected authorization type"

  let any : verifier =
   fun ctx -> try authorization ctx with _ -> unauthenticated ctx

  type t =
    | Unauthenticated
    | Admin
    | Bearer
    | Oauth
    | Refresh
    | Authorization
    | Any

  let of_t = function
    | Unauthenticated ->
        unauthenticated
    | Admin ->
        admin
    | Bearer ->
        bearer
    | Oauth ->
        oauth
    | Refresh ->
        refresh
    | Authorization ->
        authorization
    | Any ->
        any
end
