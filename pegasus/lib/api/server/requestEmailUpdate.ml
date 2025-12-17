type response = {token_required: bool [@key "tokenRequired"]}
[@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      Auth.assert_account_scope auth ~attr:Oauth.Scopes.Email
        ~action:Oauth.Scopes.Manage ;
      let did = Auth.get_authed_did_exn auth in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor ->
          let token_required = Option.is_some actor.email_confirmed_at in
          let%lwt () =
            if token_required then (
              let code =
                "eml-"
                ^ String.sub
                    Digestif.SHA256.(
                      digest_string (did ^ Int.to_string @@ Util.now_ms ())
                      |> to_hex )
                    0 8
              in
              let expires_at = Util.now_ms () + (10 * 60 * 1000) in
              let%lwt () = Data_store.set_auth_code ~did ~code ~expires_at db in
              Dream.log "email update code for %s: %s" did code ;
              Lwt.return_unit )
            else Lwt.return_unit
          in
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson {token_required} )
