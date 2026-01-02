open Lexicons.Com_atproto_server_createSession.Main

let consume_points = 1

let complete_login (actor : Data_store.Types.actor) =
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
       ; email_confirmed= Some (Option.is_some actor.email_confirmed_at)
       ; email_auth_factor=
           Some (actor.email_2fa_enabled = 1 || actor.totp_verified_at <> None)
       ; active
       ; status
       ; did_doc= None }

let verify_2fa_code ~(actor : Data_store.Types.actor) ~code db =
  let did = actor.did in
  let%lwt totp_valid = Totp.verify_login_code ~did ~code db in
  if totp_valid then Lwt.return_ok ()
  else
    let%lwt backup_valid = Totp.Backup_codes.verify_and_consume ~did ~code db in
    if backup_valid then Lwt.return_ok ()
    else
      match%lwt Two_factor.verify_email_code_by_did ~did ~code db with
      | Ok _ ->
          Lwt.return_ok ()
      | Error e ->
          Lwt.return_error e

let handler =
  Xrpc.handler (fun {req; db; _} ->
      let%lwt {identifier; password; auth_factor_token; _} =
        Xrpc.parse_body req input_of_yojson
      in
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
      | Ok (Some actor) -> (
          let is_2fa_enabled =
            actor.email_2fa_enabled = 1 || actor.totp_verified_at <> None
          in
          if not is_2fa_enabled then complete_login actor
          else
            match auth_factor_token with
            | Some token when token <> "" -> (
              match%lwt verify_2fa_code ~actor ~code:token db with
              | Ok () ->
                  complete_login actor
              | Error msg ->
                  Errors.auth_required ~name:"InvalidToken" msg )
            | _ ->
                (* no token provided, need to request 2FA *)
                let%lwt methods =
                  Two_factor.get_available_methods ~did:actor.did db
                in
                (* only send code to email if email is the only method *)
                let%lwt () =
                  if methods.email && not methods.totp then
                    let%lwt session_token =
                      Two_factor.create_pending_session ~did:actor.did db
                    in
                    let%lwt () =
                      Two_factor.send_email_code ~session_token ~actor db
                    in
                    Lwt.return ()
                  else Lwt.return ()
                in
                Errors.auth_required ~name:"AuthFactorTokenRequired"
                  "Two-factor authentication required" )
      | Ok _ ->
          Errors.invalid_request "invalid credentials"
      | Error e ->
          Errors.(log_exn e ; exn_to_response e) )
