type request =
  { client_id: string
  ; response_type: string
  ; redirect_uri: string
  ; scope: string
  ; state: string
  ; code_challenge: string
  ; code_challenge_method: string
  ; login_hint: string option }
[@@deriving yojson]

let handler ~nonce_state =
  Xrpc.handler (fun ctx ->
      let%lwt proof =
        Oauth.Dpop.verify_dpop_proof ~nonce_state
          ~mthd:(Dream.method_to_string @@ Dream.method_ ctx.req)
          ~url:(Dream.target ctx.req)
          ~dpop_header:(Dream.header ctx.req "DPoP")
          ()
      in
      match proof with
      | Error "use_dpop_nonce" ->
          let nonce = Oauth.Dpop.next_nonce nonce_state in
          Dream.json ~status:`Bad_Request ~headers:[("DPoP-Nonce", nonce)]
          @@ Yojson.Safe.to_string
          @@ `Assoc [("error", `String "use_dpop_nonce")]
      | Error e ->
          Errors.invalid_request e
      | Ok proof ->
          let%lwt req = Xrpc.parse_body ctx.req request_of_yojson in
          let%lwt client =
            try%lwt Oauth.Client.fetch_client_metadata req.client_id
            with e ->
              Errors.log_exn ~req:ctx.req e ;
              Errors.invalid_request "failed to fetch client metadata"
          in
          if req.response_type <> "code" then
            Errors.invalid_request "only response_type=code supported"
          else if req.code_challenge_method <> "S256" then
            Errors.invalid_request "only code_challenge_method=S256 supported"
          else if not (List.mem req.redirect_uri client.redirect_uris) then
            Errors.invalid_request "invalid redirect_uri"
          else
            let request_id =
              "req-" ^ (Uuidm.v4_gen (Random.get_state ()) () |> Uuidm.to_string)
            in
            let request_uri =
              "urn:ietf:params:oauth:request_uri:" ^ request_id
            in
            let expires_at =
              Util.now_ms () + Oauth.Constants.par_request_ttl_ms
            in
            let%lwt () =
              Oauth.Queries.insert_par_request ctx.db
                { request_id
                ; client_id= req.client_id
                ; request_data= Yojson.Safe.to_string (request_to_yojson req)
                ; dpop_jkt= Some proof.jkt
                ; expires_at
                ; created_at= Util.now_ms () }
            in
            Dream.json ~status:`Created
              ~headers:[("DPoP-Nonce", Oauth.Dpop.next_nonce nonce_state)]
            @@ Yojson.Safe.to_string
            @@ `Assoc
                 [("request_uri", `String request_uri); ("expires_in", `Int 300)] )
