open Oauth
open Oauth.Types

let options_handler = Xrpc.handler (fun _ -> Dream.empty `No_Content)

let post_handler =
  Xrpc.handler ~auth:DPoP (fun ctx ->
      let proof = Auth.get_dpop_proof_exn ctx.auth in
      let%lwt req = Xrpc.parse_body ctx.req par_request_of_yojson in
      let%lwt client =
        try%lwt Client.fetch_client_metadata req.client_id
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
          "req-"
          ^ Uuidm.to_string (Uuidm.v4_gen (Random.State.make_self_init ()) ())
        in
        let request_uri = Constants.request_uri_prefix ^ request_id in
        let expires_at = Util.now_ms () + Constants.par_request_ttl_ms in
        let request : oauth_request =
          { request_id
          ; client_id= req.client_id
          ; request_data= Yojson.Safe.to_string (par_request_to_yojson req)
          ; dpop_jkt= Some proof.jkt
          ; expires_at
          ; created_at= Util.now_ms () }
        in
        let%lwt () = Queries.insert_par_request ctx.db request in
        Dream.json ~status:`Created
        @@ Yojson.Safe.to_string
        @@ `Assoc
             [("request_uri", `String request_uri); ("expires_in", `Int 300)] )
