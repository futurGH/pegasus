let handler =
  Xrpc.handler ~auth:Auth.Verifiers.authorization (fun {req; db; auth} ->
      let did = Auth.get_authed_did_exn auth in
      let%lwt body = Dream.body req in
      let prefs =
        match Yojson.Safe.from_string body with
        | `Assoc [("preferences", prefs)] ->
            prefs
        | _ ->
            Errors.invalid_request "Invalid request body"
      in
      let%lwt () = Data_store.put_preferences ~did ~prefs db in
      Dream.empty `OK )
