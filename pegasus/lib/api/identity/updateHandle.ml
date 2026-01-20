open Lexicons.Com.Atproto.Identity.UpdateHandle.Main

let calc_key_did ctx = Some (Auth.get_authed_did_exn ctx.Xrpc.auth)

let handler =
  Xrpc.handler ~auth:Authorization
    ~rate_limits:
      [ Route
          { duration_ms= 5 * Util.Time.minute
          ; points= 10
          ; calc_key= Some calc_key_did
          ; calc_points= None }
      ; Route
          { duration_ms= Util.Time.day
          ; points= 50
          ; calc_key= Some calc_key_did
          ; calc_points= None } ]
    (fun {req; auth; db; _} ->
      Auth.assert_identity_scope auth ~attr:Oauth.Scopes.Handle ;
      let did = Auth.get_authed_did_exn auth in
      let%lwt {handle} = Xrpc.parse_body req input_of_yojson in
      match%lwt Identity_util.update_handle ~did ~handle db with
      | Ok () ->
          Dream.empty `OK
      | Error e ->
          let msg = Identity_util.update_handle_error_to_string e in
          Log.err (fun log -> log "%s" msg) ;
          Errors.invalid_request ~name:"InvalidHandle" msg )
