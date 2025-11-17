open struct
  let make_url pth =
    Uri.(make ~scheme:"https" ~host:Env.hostname ~path:pth () |> to_string)

  let pds_url = `String (make_url "")
end

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
                 ; ("serviceEndpoint", pds_url) ] ) ] )

let oauth_protected_resource =
  Xrpc.handler (fun _ ->
      Dream.json @@ Yojson.Safe.to_string
      @@ `Assoc
           [ ("authorization_servers", `List [pds_url])
           ; ("bearer_methods_supported", `List [`String "header"])
           ; ("resource", pds_url)
           ; ("resource_documentation", `String "https://atproto.com")
           ; ("scopes_supported", `List []) ] )

let oauth_authorization_server =
  Xrpc.handler (fun _ ->
      Dream.json @@ Yojson.Safe.to_string
      @@ `Assoc
           [ ("issuer", pds_url)
           ; ("authorization_endpoint", `String (make_url "/oauth/authorize"))
           ; ("token_endpoint", `String (make_url "/oauth/token"))
           ; ( "pushed_authorization_request_endpoint"
             , `String (make_url "/oauth/par") )
           ; ("require_pushed_authorization_requests", `Bool true)
           ; ( "scopes_supported"
             , `List
                 [ `String "atproto"
                 ; `String "transition:email"
                 ; `String "transition:generic"
                 ; `String "transition:chat.bsky" ] )
           ; ("subject_types_supported", `List [`String "public"])
           ; ("response_types_supported", `List [`String "code"])
           ; ( "response_modes_supported"
             , `List [`String "query"; `String "fragment"] )
           ; ( "grant_types_supported"
             , `List [`String "authorization_code"; `String "refresh_token"] )
           ; ("code_challenge_methods_supported", `List [`String "S256"])
           ; ("ui_locales_supported", `List [`String "en-US"])
           ; ( "display_values_supported"
             , `List [`String "page"; `String "popup"; `String "touch"] )
           ; ("authorization_response_iss_parameter_supported", `Bool true)
           ; ( "request_object_signing_alg_values_supported"
             , `List [`String "ES256"; `String "ES256K"] )
           ; ("request_object_encryption_alg_values_supported", `List [])
           ; ("request_object_encryption_enc_values_supported", `List [])
           ; ( "token_endpoint_auth_methods_supported"
             , `List [`String "none"; `String "private_key_jwt"] )
           ; ( "token_endpoint_auth_signing_alg_values_supported"
             , `List [`String "ES256"; `String "ES256K"] )
           ; ( "dpop_signing_alg_values_supported"
             , `List [`String "ES256"; `String "ES256K"] )
           ; ("client_id_metadata_document_supported", `Bool true) ] )
