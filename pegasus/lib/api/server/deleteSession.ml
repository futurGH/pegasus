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
      Dream.empty `OK )
