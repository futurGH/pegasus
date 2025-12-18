type t = (module Rapper_helper.CONNECTION)

type session_info =
  { handle: string
  ; did: string
  ; email: string
  ; email_confirmed: bool [@key "emailConfirmed"]
  ; email_auth_factor: bool [@key "emailAuthFactor"]
  ; active: bool option [@default None]
  ; status: string option [@default None] }
[@@deriving yojson {strict= false}]

type credentials =
  | Unauthenticated
  | Admin
  | Access of {did: string}
  | Refresh of {did: string; jti: string}
  | OAuth of {did: string; proof: Oauth.Dpop.proof; scopes: Oauth.Scopes.t}
  | DPoP of {proof: Oauth.Dpop.proof}

let verify_bearer_jwt t token expected_scope =
  match Jwt.verify_jwt token ~pubkey:Env.jwt_pubkey with
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
  | (Access {did= creds} | OAuth {did= creds; _}) when creds = did ->
      true
  | Refresh {did= creds; _} when creds = did && refresh ->
      true
  | _ ->
      false

let get_scopes = function OAuth {scopes; _} -> Some scopes | _ -> None

let is_oauth = function OAuth _ -> true | _ -> false

let assert_repo_scope credentials ~collection ~action =
  match credentials with
  | OAuth {scopes; _} ->
      if not (Oauth.Scopes.Transition.allows_repo scopes {collection; action})
      then
        Errors.auth_required ~name:"InsufficientScope"
          ( "scope doesn't allow "
          ^ Oauth.Scopes.show_repo_action action
          ^ " on " ^ collection )
  | _ ->
      ()

let assert_blob_scope credentials ~mime =
  match credentials with
  | OAuth {scopes; _} ->
      if not (Oauth.Scopes.Transition.allows_blob scopes {mime}) then
        Errors.auth_required ~name:"InsufficientScope"
          ("scope doesn't allow blob upload for " ^ mime)
  | _ ->
      ()

let assert_identity_scope credentials ~attr =
  match credentials with
  | OAuth {scopes; _} ->
      if not (Oauth.Scopes.allows_identity scopes {attr}) then
        Errors.auth_required ~name:"InsufficientScope"
          "scope doesn't allow identity operations"
  | _ ->
      ()

let assert_account_scope credentials ~attr ~action =
  match credentials with
  | OAuth {scopes; _} ->
      if not (Oauth.Scopes.Transition.allows_account scopes {attr; action}) then
        Errors.auth_required ~name:"InsufficientScope"
          "scope doesn't allow account operations"
  | _ ->
      ()

let allows_email_read credentials =
  match credentials with
  | OAuth {scopes; _} ->
      Oauth.Scopes.Transition.allows_account scopes
        {attr= Oauth.Scopes.Email; action= Oauth.Scopes.Read}
  | _ ->
      true

let assert_rpc_scope credentials ~lxm ~aud =
  match credentials with
  | OAuth {scopes; _} ->
      if not (Oauth.Scopes.Transition.allows_rpc scopes {lxm; aud}) then
        Errors.auth_required ~name:"InsufficientScope"
          ("scope doesn't allow rpc call to " ^ lxm)
  | _ ->
      ()

let get_authed_did_exn = function
  | Access {did} | OAuth {did; _} ->
      did
  | Refresh {did; _} ->
      did
  | _ ->
      Errors.auth_required "invalid authorization header"

let get_dpop_proof_exn = function
  | OAuth {proof; _} | DPoP {proof} ->
      proof
  | _ ->
      Errors.invalid_request "invalid DPoP header"

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
      match Dream.header req "Authorization" with
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
  end

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

  let parse_dpop req = parse_header req "DPoP"

  type ctx = {req: Dream.request; db: Data_store.t}

  type verifier = ctx -> (credentials, exn) Lwt_result.t

  let unauthenticated : verifier = fun _ -> Lwt.return_ok Unauthenticated

  let admin : verifier =
   fun {req; _} ->
    match parse_basic req with
    | Ok (username, password) -> (
      match (username, password) with
      | "admin", p when p = Env.admin_password ->
          Lwt.return_ok Admin
      | _ ->
          Lwt.return_error @@ Errors.auth_required "invalid credentials" )
    | Error _ ->
        Lwt.return_error @@ Errors.auth_required "invalid authorization header"

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
                 "account is deactivated"
        | None ->
            Lwt.return_error @@ Errors.auth_required "invalid credentials" )
      | Error _ ->
          Lwt.return_error @@ Errors.auth_required "invalid credentials" )
    | Error _ ->
        Lwt.return_error @@ Errors.auth_required "invalid authorization header"

  let dpop : verifier =
   fun {req; _} ->
    let dpop_header = Dream.header req "DPoP" in
    match
      Oauth.Dpop.verify_dpop_proof
        ~mthd:(Dream.method_to_string @@ Dream.method_ req)
        ~url:(Dream.target req) ~dpop_header ()
    with
    | Error "use_dpop_nonce" ->
        Lwt.return_error @@ Errors.use_dpop_nonce ()
    | Error e ->
        Lwt.return_error @@ Errors.invalid_request ("dpop error: " ^ e)
    | Ok proof ->
        Lwt.return_ok (DPoP {proof})

  let oauth : verifier =
   fun {req; db} ->
    match parse_dpop req with
    | Error e ->
        Lwt.return_error @@ Errors.invalid_request ("dpop error: " ^ e)
    | Ok token -> (
      match
        Oauth.Dpop.verify_dpop_proof
          ~mthd:(Dream.method_to_string @@ Dream.method_ req)
          ~url:(Dream.target req) ~dpop_header:(Dream.header req "DPoP")
          ~access_token:token ()
      with
      | Error "use_dpop_nonce" ->
          Lwt.return_error @@ Errors.use_dpop_nonce ()
      | Error e ->
          Lwt.return_error @@ Errors.invalid_request ("dpop error: " ^ e)
      | Ok proof -> (
        match Jwt.verify_jwt token ~pubkey:Env.jwt_pubkey with
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
              let scope_str =
                claims |> member "scope" |> to_string_option
                |> Option.value ~default:""
              in
              let scopes = Oauth.Scopes.of_string scope_str in
              let now = int_of_float (Unix.gettimeofday ()) in
              if jkt_claim <> proof.jkt then
                Lwt.return_error @@ Errors.auth_required "dpop key mismatch"
              else if exp < now then
                Lwt.return_error @@ Errors.auth_required "token expired"
              else if not (Oauth.Scopes.has_atproto scopes) then
                Lwt.return_error
                @@ Errors.auth_required ~name:"InvalidToken"
                     "oauth token missing 'atproto' scope"
              else
                let%lwt session =
                  try%lwt
                    let%lwt sess = get_session_info did db in
                    Lwt.return_ok sess
                  with _ ->
                    Lwt.return_error
                    @@ Errors.auth_required "invalid credentials"
                in
                match session with
                | Ok {active= Some true; _} ->
                    Lwt.return_ok (OAuth {did; proof; scopes})
                | Ok _ ->
                    Lwt.return_error
                    @@ Errors.auth_required ~name:"AccountDeactivated"
                         "account is deactivated"
                | Error _ ->
                    Lwt.return_error
                    @@ Errors.auth_required "invalid credentials"
            with _ ->
              Lwt.return_error @@ Errors.auth_required "malformed JWT claims" )
        ) )

  let session : verifier =
   fun {req; db} ->
    match%lwt Session.Raw.get_current_did req with
    | Some did -> (
      match%lwt Data_store.get_actor_by_identifier did db with
      | Some {deactivated_at= None; _} ->
          Lwt.return_ok (Access {did})
      | Some {deactivated_at= Some _; _} when Dream.target req = "/account" ->
          Lwt.return_ok (Access {did})
      | Some {deactivated_at= Some _; _}
        when String.starts_with ~prefix:"/account" (Dream.target req) ->
          raise (Errors.Redirect "/account")
      | Some {deactivated_at= Some _; _} ->
          Lwt.return_error
          @@ Errors.auth_required ~name:"AccountDeactivated"
               "account is deactivated"
      | None ->
          Lwt.return_error @@ Errors.auth_required "invalid session" )
    | None ->
        Lwt.return_error @@ Errors.auth_required "no active session"

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
                 "account is deactivated"
        | None ->
            Lwt.return_error @@ Errors.auth_required "invalid credentials" )
      | Error "" | Error _ ->
          Lwt.return_error @@ Errors.auth_required "invalid credentials" )
    | Error _ ->
        Lwt.return_error @@ Errors.auth_required "invalid authorization header"

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
    | _ -> (
      match%lwt session ctx with
      | Ok creds ->
          Lwt.return_ok creds
      | Error _ ->
          Lwt.return_error
          @@ Errors.auth_required ~name:"InvalidToken"
               "unexpected authorization type" )

  let any : verifier =
   fun ctx -> try authorization ctx with _ -> unauthenticated ctx

  type t =
    | Unauthenticated
    | Admin
    | Bearer
    | DPoP
    | OAuth
    | Refresh
    | Session
    | Authorization
    | Any

  let of_t = function
    | Unauthenticated ->
        unauthenticated
    | Admin ->
        admin
    | Bearer ->
        bearer
    | DPoP ->
        dpop
    | OAuth ->
        oauth
    | Refresh ->
        refresh
    | Session ->
        session
    | Authorization ->
        authorization
    | Any ->
        any
end
