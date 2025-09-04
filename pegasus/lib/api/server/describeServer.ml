let handler =
  Xrpc.handler (fun _ ->
      Dream.json @@ Yojson.Safe.to_string
      @@ `Assoc
           [ ("did", `String ("did:web:" ^ Env.hostname))
           ; ("availableUserDomains", `List [`String ("." ^ Env.hostname)])
           ; ("inviteCodeRequired", `Bool Env.invite_required)
           ; ("links", `Assoc [])
           ; ("contact", `Assoc []) ] )
