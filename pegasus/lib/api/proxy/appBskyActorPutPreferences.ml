let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      let%lwt body = Dream.body req in
      let prefs =
        match Yojson.Safe.from_string body with
        | `Assoc [("preferences", `List prefs)] ->
            prefs
        | _ ->
            Errors.invalid_request "invalid request body"
      in
      let%lwt () = Data_store.put_preferences ~did ~prefs:(`List prefs) db in
      Dream.empty `OK )
