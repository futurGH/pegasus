type response = Auth.session_info

let handler =
  Xrpc.handler ~auth:Authorization (fun {db; auth; _} ->
      let did = Auth.get_authed_did_exn auth in
      let%lwt session = Auth.get_session_info did db in
      Dream.json @@ Yojson.Safe.to_string @@ Auth.session_info_to_yojson session )
