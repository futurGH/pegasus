open Lexicons.Com_atproto_sync_listRepos
open Main

let handler =
  Xrpc.handler (fun ctx ->
      let {cursor; limit} = Xrpc.parse_query ctx.req params_of_yojson in
      let limit =
        match limit with
        | Some limit when limit > 0 && limit <= 1000 ->
            limit
        | _ ->
            1000
      in
      let%lwt actors = Data_store.list_actors ?cursor ~limit ctx.db in
      let%lwt repos =
        List.map
          (fun (a : Data_store.Types.actor) ->
            let%lwt user_db = User_store.connect a.did in
            let%lwt head, {rev; _} =
              match%lwt User_store.get_commit user_db with
              | Some c ->
                  Lwt.return c
              | None ->
                  failwith ("failed to retrieve commit for " ^ a.did)
            in
            let active, status =
              match a.deactivated_at with
              | Some _ ->
                  (Some false, Some "deactivated")
              | None ->
                  (Some true, None)
            in
            Lwt.return
              {did= a.did; head= Cid.to_string head; rev; active; status} )
          actors
        |> Lwt.all
      in
      let cursor =
        if List.length repos = limit then
          Option.map (fun r -> r.did) @@ Mist.Util.last repos
        else None
      in
      Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson {cursor; repos} )
