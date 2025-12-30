let get_credentials did ?(extra_rotation_keys = []) db =
  match%lwt Data_store.get_actor_by_identifier did db with
  | None ->
      Lwt.return_error "actor not found"
  | Some actor ->
      actor.signing_key |> Kleidos.parse_multikey_str |> Kleidos.derive_pubkey
      |> Kleidos.pubkey_to_did_key
      |> (fun did_key ->
      Plc.create_did_credentials Env.rotation_key did_key actor.handle
        ~rotation_did_keys:extra_rotation_keys )
      |> Lwt.return_ok

let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      match%lwt get_credentials did db with
      | Error msg ->
          Errors.internal_error ~msg ()
      | Ok credentials ->
          credentials |> Plc.credentials_to_yojson |> Yojson.Safe.to_string
          |> Dream.json )
