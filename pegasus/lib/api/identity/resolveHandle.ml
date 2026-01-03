open Lexicons.Com.Atproto.Identity.ResolveHandle.Main

let handler =
  Xrpc.handler (fun ctx ->
      let {handle} = Xrpc.parse_query ctx.req params_of_yojson in
      match%lwt Data_store.get_actor_by_identifier handle ctx.db with
      | Some actor ->
          Dream.json @@ Yojson.Safe.to_string
          @@ output_to_yojson {did= actor.did}
      | None -> (
        match%lwt Id_resolver.Handle.resolve handle with
        | Ok did ->
            Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson {did}
        | Error e -> (
          try%lwt Xrpc.service_proxy ctx
          with _ ->
            Log.err (fun log -> log "%s" e) ;
            Errors.internal_error ~msg:"could not resolve handle" () ) ) )
