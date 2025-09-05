type response = Auth.session_info

let handler =
  Xrpc.handler ~auth:Auth.Verifiers.access (fun {db; auth; _} ->
      let did =
        match auth with Access {did} -> did | _ -> failwith "non-access auth"
      in
      let%lwt session = Auth.get_session_info did db in
      Dream.json @@ Yojson.Safe.to_string @@ Auth.session_info_to_yojson session )
