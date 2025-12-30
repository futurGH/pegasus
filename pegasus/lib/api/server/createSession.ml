open Lexicons.Com_atproto_server_createSession.Main

let consume_points = 1

let handler =
  Xrpc.handler (fun {req; db; _} ->
      let%lwt {identifier; password; _} = Xrpc.parse_body req input_of_yojson in
      let id = String.lowercase_ascii identifier in
      (* apply rate limits after parsing body so we can create key from identifier *)
      let key = id ^ "-" ^ Util.request_ip req in
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
          @@ output_to_yojson
               { access_jwt
               ; refresh_jwt
               ; handle= actor.handle
               ; did= actor.did
               ; email= Some actor.email
               ; email_confirmed= Some true
               ; email_auth_factor= Some true
               ; active
               ; status
               ; did_doc= None }
      | Ok _ ->
          Errors.invalid_request "invalid credentials"
      | Error e ->
          Errors.(log_exn e ; exn_to_response e) )
