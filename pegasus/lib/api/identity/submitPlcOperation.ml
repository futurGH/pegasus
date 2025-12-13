type request = {operation: Plc.signed_operation_op}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      Auth.assert_identity_scope auth ~attr:Oauth.Scopes.Any ;
      if not (String.starts_with ~prefix:"did:plc:" did) then
        Errors.invalid_request "this method is only for did:plc identities" ;
      let%lwt {operation= op} = Xrpc.parse_body req request_of_yojson in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor -> (
          let pds_pubkey =
            Env.rotation_key |> Kleidos.derive_pubkey
            |> Kleidos.pubkey_to_did_key
          in
          if not (List.mem pds_pubkey op.rotation_keys) then
            Errors.invalid_request
              "rotation keys must include the PDS public key" ;
          ( match List.assoc_opt "atproto_pds" op.services with
          | Some {type'; endpoint}
            when type' <> "AtprotoPersonalDataServer"
                 || endpoint <> Env.host_endpoint ->
              Errors.invalid_request "invalid atproto_pds service"
          | _ ->
              () ) ;
          let actor_pubkey =
            actor.signing_key |> Kleidos.parse_multikey_str
            |> Kleidos.derive_pubkey |> Kleidos.pubkey_to_did_key
          in
          if
            List.assoc_opt "atproto" op.verification_methods
            <> Some actor_pubkey
          then Errors.invalid_request "incorrect atproto signing key" ;
          if List.hd op.also_known_as <> "at://" ^ actor.handle then
            Errors.invalid_request "incorrect handle" ;
          match%lwt Plc.submit_operation did (Operation op) with
          | Ok () ->
              let%lwt _ = Sequencer.sequence_identity db ~did () in
              let%lwt _ = Id_resolver.Did.resolve did in
              Dream.empty `OK
          | Error (status, msg) ->
              Errors.internal_error
                ~msg:
                  ( "failed to submit plc operation: " ^ Int.to_string status
                  ^ " " ^ msg )
                () ) )
