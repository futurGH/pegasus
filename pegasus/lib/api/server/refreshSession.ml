open Lexicons.Com.Atproto.Server.RefreshSession.Main

let handler =
  Xrpc.handler ~auth:Refresh (fun {db; auth; _} ->
      let did, jti =
        match auth with
        | Refresh {did; jti} ->
            (did, jti)
        | _ ->
            failwith "non-refresh auth"
      in
      let%lwt () = Data_store.revoke_token ~did ~jti db in
      let%lwt {handle; did; active; status; _} = Auth.get_session_info did db in
      let access_jwt, refresh_jwt = Jwt.generate_jwt did in
      Dream.json @@ Yojson.Safe.to_string
      @@ output_to_yojson
           {access_jwt; refresh_jwt; handle; did; active; status; did_doc= None} )
