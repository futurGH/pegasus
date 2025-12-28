type query = {handle: string} [@@deriving yojson {strict= false}]

type response = {did: string} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun ctx ->
      let {handle} = Xrpc.parse_query ctx.req query_of_yojson in
      match%lwt Data_store.get_actor_by_identifier handle ctx.db with
      | Some actor ->
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson {did= actor.did}
      | None -> (
        match%lwt Id_resolver.Handle.resolve handle with
        | Ok did ->
            Dream.json @@ Yojson.Safe.to_string @@ response_to_yojson {did}
        | Error e -> (
          try%lwt Xrpc.service_proxy ctx
          with _ ->
            Dream.error (fun log -> log "%s" e) ;
            Errors.internal_error ~msg:"could not resolve handle" () ) ) )
