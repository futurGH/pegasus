let handler =
  Xrpc.handler ~auth:Authorization (fun {auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some {handle; signing_key; _} ->
          Plc.get_recommended_credentials ~signing_key ~handle ()
          |> Plc.credentials_to_yojson |> Yojson.Safe.to_string |> Dream.json )
