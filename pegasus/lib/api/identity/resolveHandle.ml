type response = {did: string} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun {req; db; _} ->
      let handle =
        match Dream.query req "handle" with
        | Some handle ->
            handle
        | None ->
            Errors.invalid_request "missing parameter 'handle'"
      in
      match%lwt Data_store.get_actor_by_identifier handle db with
      | Some actor ->
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson {did= actor.did}
      | None -> (
        match%lwt Id_resolver.Handle.resolve handle with
        | Ok did ->
            Dream.json @@ Yojson.Safe.to_string @@ response_to_yojson {did}
        | Error e ->
            Dream.error (fun log -> log "%s" e) ;
            Errors.internal_error ~msg:"could not resolve handle" () ) )
