type request =
  { identifier: string
  ; password: string
  ; auth_factor_token: string option [@key "authFactorToken"] }
[@@deriving yojson {strict= false}]

type response =
  { access_jwt: string [@key "accessJwt"]
  ; refresh_jwt: string [@key "refreshJwt"]
  ; handle: string
  ; did: string
  ; email: string
  ; email_confirmed: bool [@key "emailConfirmed"]
  ; email_auth_factor: bool [@key "emailAuthFactor"]
  ; active: bool option
  ; status: string option }
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Auth.Verifiers.authorization (fun {req; db; auth} ->
      let%lwt {identifier; password; _} =
        Xrpc.parse_body req request_of_yojson
      in
      let id = String.lowercase_ascii identifier in
      match%lwt
        Lwt_result.catch @@ fun () -> Data_store.try_login ~id ~password db
      with
      | Ok (Some actor) when Auth.verify_auth auth actor.did ->
          let access_jwt, refresh_jwt = Auth.generate_jwt actor.did in
          let active, status =
            match actor.deactivated_at with
            | None ->
                (Some true, None)
            | Some _ ->
                (Some false, Some "deactivated")
          in
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson
               { access_jwt
               ; refresh_jwt
               ; handle= actor.handle
               ; did= actor.did
               ; email= actor.email
               ; email_confirmed= true
               ; email_auth_factor= true
               ; active
               ; status }
      | Ok _ ->
          Errors.invalid_request "Invalid credentials"
      | Error e ->
          Errors.(log_exn e ; exn_to_response e) )
