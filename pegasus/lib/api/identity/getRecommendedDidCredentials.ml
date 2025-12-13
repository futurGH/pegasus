type response = Plc.credentials

let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor ->
          actor.signing_key |> Kleidos.parse_multikey_str
          |> Kleidos.derive_pubkey |> Kleidos.pubkey_to_did_key
          |> (fun did_key ->
          Plc.create_did_credentials Env.rotation_key did_key actor.handle )
          |> Plc.credentials_to_yojson |> Yojson.Safe.to_string |> Dream.json )
