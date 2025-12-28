type response = Auth.session_info [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Authorization (fun {db; auth; _} ->
      let did = Auth.get_authed_did_exn auth in
      let%lwt session = Auth.get_session_info did db in
      (* strip email fields if oauth token doesn't have email read permission *)
      let session =
        if Auth.allows_email_read auth then session
        else
          { session with
            email= Some ""
          ; email_confirmed= Some false
          ; email_auth_factor= Some false }
      in
      Dream.json @@ Yojson.Safe.to_string @@ Auth.session_info_to_yojson session )
