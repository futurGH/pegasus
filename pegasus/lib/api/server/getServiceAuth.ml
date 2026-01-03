open Lexicons.Com.Atproto.Server.GetServiceAuth.Main

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      let aud, lxm =
        match (Dream.query req "aud", Dream.query req "lxm") with
        | Some aud, Some lxm ->
            (aud, lxm)
        | Some aud, None ->
            (aud, "*")
        | _ ->
            Errors.invalid_request "missing aud"
      in
      Auth.assert_rpc_scope auth ~lxm ~aud ;
      let%lwt signing_multikey =
        match%lwt Data_store.get_actor_by_identifier did db with
        | Some {signing_key; _} ->
            Lwt.return signing_key
        | None ->
            Errors.internal_error ~msg:"actor not found" ()
      in
      let signing_key = Kleidos.parse_multikey_str signing_multikey in
      let token = Jwt.generate_service_jwt ~did ~aud ~lxm ~signing_key in
      Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson {token} )
