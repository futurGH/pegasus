type request =
  { identifier: string
  ; password: string
  ; auth_factor_token: string option [@key "authFactorToken"] [@default None] }
[@@deriving yojson {strict= false}]

type response =
  { access_jwt: string [@key "accessJwt"]
  ; refresh_jwt: string [@key "refreshJwt"]
  ; handle: string
  ; did: string
  ; email: string
  ; email_confirmed: bool [@key "emailConfirmed"]
  ; email_auth_factor: bool [@key "emailAuthFactor"]
  ; active: bool option [@default None]
  ; status: string option [@default None] }
[@@deriving yojson {strict= false}]

let consume_points = 1

let handler =
  Xrpc.handler (fun {req; db; _} ->
      let%lwt {identifier; password; _} =
        Xrpc.parse_body req request_of_yojson
      in
      let id = String.lowercase_ascii identifier in
      (* apply rate limits after parsing body so we can create key from identifier *)
      let key = id ^ "-" ^ Dream.client req in
      let _ =
        Xrpc.consume_route_rate_limit ~name:"repo-write-hour"
          ~duration_ms:Util.day ~max_points:300 ~key ~consume_points
      in
      let _ =
        Xrpc.consume_route_rate_limit ~name:"repo-write-day"
          ~duration_ms:(5 * Util.minute) ~max_points:30 ~key ~consume_points
      in
      match%lwt
        Lwt_result.catch @@ fun () -> Data_store.try_login ~id ~password db
      with
      | Ok (Some actor) ->
          let access_jwt, refresh_jwt = Jwt.generate_jwt actor.did in
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
          Errors.invalid_request "invalid credentials"
      | Error e ->
          Errors.(log_exn e ; exn_to_response e) )
