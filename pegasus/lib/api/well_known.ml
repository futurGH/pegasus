let did_json =
  Xrpc.handler (fun _ ->
      Dream.json @@ Yojson.Safe.to_string
      @@ `Assoc
           [ ("@context", `List [`String "https://www.w3.org/ns/did/v1"])
           ; ("id", `String Env.did)
           ; ( "service"
             , `Assoc
                 [ ("id", `String "#atproto_pds")
                 ; ("type", `String "AtprotoPersonalDataServer")
                 ; ("serviceEndpoint", `String ("https://" ^ Env.hostname)) ] )
           ] )
