type t = (module Rapper_helper.CONNECTION)

type symmetric_jwt =
  {scope: string; aud: string; sub: string; iat: int; exp: int; jti: string}

type credentials =
  | Unauthenticated
  | Admin
  | Access of {did: string}
  | Refresh of {did: string; jti: string}

let generate_jwt did =
  let now_s = int_of_float (Unix.gettimeofday ()) in
  let access_exp = now_s + (60 * 60 * 3) in
  let refresh_exp = now_s + (60 * 60 * 24 * 7) in
  let jti = Uuidm.v4_gen (Random.get_state ()) () |> Uuidm.to_string in
  let access =
    match
      Jwto.encode Jwto.HS256 Env.jwt_secret
        [ ("scope", "com.atproto.access")
        ; ("aud", Env.did)
        ; ("sub", did)
        ; ("iat", Int.to_string now_s)
        ; ("exp", Int.to_string access_exp)
        ; ("jti", jti) ]
    with
    | Ok token ->
        token
    | Error err ->
        failwith err
  in
  let refresh =
    match
      Jwto.encode Jwto.HS256 Env.jwt_secret
        [ ("scope", "com.atproto.refresh")
        ; ("aud", Env.did)
        ; ("sub", did)
        ; ("iat", Int.to_string now_s)
        ; ("exp", Int.to_string refresh_exp)
        ; ("jti", jti) ]
    with
    | Ok token ->
        token
    | Error err ->
        failwith err
  in
  (access, refresh)

let verify_bearer_jwt t token expected_scope =
  match Jwto.decode_and_verify Env.jwt_secret token with
  | Error err ->
      Lwt.return_error err
  | Ok jwt ->
      let payload = Jwto.get_payload jwt in
      let now_s = int_of_float (Unix.gettimeofday ()) in
      let scope = List.assoc_opt "scope" payload |> Option.value ~default:"" in
      let aud = List.assoc_opt "aud" payload |> Option.value ~default:"" in
      let sub = List.assoc_opt "sub" payload |> Option.value ~default:"" in
      let iat =
        List.assoc_opt "iat" payload
        |> Option.map int_of_string
        |> Option.value ~default:max_int
      in
      let exp =
        List.assoc_opt "exp" payload
        |> Option.map int_of_string |> Option.value ~default:0
      in
      let jti = List.assoc_opt "jti" payload |> Option.value ~default:"" in
      if aud <> Env.did then Lwt.return_error "invalid aud"
      else if sub = "" then Lwt.return_error "missing sub"
      else if now_s < iat then Lwt.return_error "token issued in the future"
      else if now_s > exp then Lwt.return_error "expired token"
      else if scope <> expected_scope then Lwt.return_error "invalid scope"
      else if jti = "" then Lwt.return_error "missing jti"
      else
        let%lwt revoked_at = Data_store.is_token_revoked t ~did:sub ~jti in
        if revoked_at <> None then Lwt.return_error "token revoked"
        else Lwt.return_ok {scope; aud; sub; iat; exp; jti}

module Verifiers = struct
  open Util.Exceptions

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

  let unauthenticated : verifier = function
    | {req; _} -> (
      match Dream.header req "authorization" with
      | Some _ ->
          Lwt.return_error
          @@ AuthError {error= None; message= "Invalid authorization header"}
      | None ->
          Lwt.return_ok Unauthenticated )

  let admin : verifier = function
    | {req; _} -> (
      match parse_basic req with
      | Ok (username, password) -> (
        match (username, password) with
        | "admin", p when p = Env.admin_password ->
            Lwt.return_ok Admin
        | _ ->
            Lwt.return_error
            @@ AuthError {error= None; message= "Invalid credentials"} )
      | Error _ ->
          Lwt.return_error
          @@ AuthError {error= None; message= "Invalid authorization header"} )

  let access : verifier = function
    | {req; db} -> (
      match parse_bearer req with
      | Ok jwt -> (
          match%lwt verify_bearer_jwt db jwt "com.atproto.access" with
          | Ok {sub= did; _} -> (
              match%lwt Data_store.get_actor_by_identifier did db with
              | Some {deactivated_at= None; _} ->
                  Lwt.return_ok (Access {did})
              | Some {deactivated_at= Some _; _} ->
                  Lwt.return_error
                  @@ AuthError
                       { error= Some "AccountDeactivated"
                       ; message= "Account is deactivated" }
              | None ->
                  Lwt.return_error
                  @@ AuthError {error= None; message= "Invalid credentials"} )
          | Error _ ->
              Lwt.return_error
              @@ AuthError {error= None; message= "Invalid credentials"} )
      | Error _ ->
          Lwt.return_error
          @@ AuthError {error= None; message= "Invalid authorization header"} )

  let refresh : verifier = function
    | {req; db} -> (
      match parse_bearer req with
      | Ok jwt -> (
          match%lwt verify_bearer_jwt db jwt "com.atproto.refresh" with
          | Ok {sub= did; jti; _} -> (
              match%lwt Data_store.get_actor_by_identifier did db with
              | Some {deactivated_at= None; _} ->
                  Lwt.return_ok (Refresh {did; jti})
              | Some {deactivated_at= Some _; _} ->
                  Lwt.return_error
                  @@ AuthError
                       { error= Some "AccountDeactivated"
                       ; message= "Account is deactivated" }
              | None ->
                  Lwt.return_error
                  @@ AuthError {error= None; message= "Invalid credentials"} )
          | Error "" | Error _ ->
              Lwt.return_error
              @@ AuthError {error= None; message= "Invalid credentials"} )
      | Error _ ->
          Lwt.return_error
          @@ AuthError {error= None; message= "Invalid authorization header"} )

  let authorization : verifier = function
    | ctx -> (
      match
        Dream.header ctx.req "Authorization"
        |> Option.map @@ String.split_on_char ' '
      with
      | Some ("Basic" :: _) ->
          admin ctx
      | Some ("Bearer" :: _) ->
          access ctx
      | _ ->
          Lwt.return_error
          @@ AuthError
               { error= Some "InvalidToken"
               ; message= "Unexpected authorization type" } )
end
