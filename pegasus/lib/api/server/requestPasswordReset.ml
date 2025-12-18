type request = {email: string} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun {req; auth; db; _} ->
      let%lwt actor_opt =
        match auth with
        | Auth.Access {did} | Auth.OAuth {did; _} -> (
          match%lwt Data_store.get_actor_by_identifier did db with
          | Some actor ->
              Lwt.return_some actor
          | None ->
              Errors.internal_error ~msg:"actor not found" () )
        | _ -> (
            let%lwt {email} = Xrpc.parse_body req request_of_yojson in
            let email = String.lowercase_ascii email in
            match%lwt Data_store.get_actor_by_identifier email db with
            | Some actor ->
                Lwt.return_some actor
            | None ->
                Dream.log "password reset requested for unknown email: %s" email ;
                Lwt.return_none )
      in
      match actor_opt with
      | None ->
          (* always return success to prevent email enumeration *)
          Dream.empty `OK
      | Some actor ->
          let code =
            "pwd-"
            ^ String.sub
                Digestif.SHA256.(
                  digest_string (actor.did ^ Int.to_string @@ Util.now_ms ())
                  |> to_hex )
                0 8
          in
          let expires_at = Util.now_ms () + (10 * 60 * 1000) in
          let%lwt () =
            Data_store.set_auth_code ~did:actor.did ~code ~expires_at db
          in
          let%lwt () =
            Util.send_email_or_log ~recipients:[To actor.email]
              ~subject:(Printf.sprintf "Password reset for %s" actor.handle)
              ~body:
                (Plain
                   (Printf.sprintf
                      "Reset your password using the following token: %s" code )
                )
          in
          Dream.empty `OK )
