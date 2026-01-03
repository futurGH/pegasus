open Lexicons.Com.Atproto.Sync.GetRepoStatus.Main

let handler =
  Xrpc.handler (fun ctx ->
      let {did} : params = Xrpc.parse_query ctx.req params_of_yojson in
      let%lwt actor =
        match%lwt Data_store.get_actor_by_identifier did ctx.db with
        | Some actor ->
            Lwt.return actor
        | None ->
            Errors.invalid_request ~name:"RepoNotFound"
              "couldn't find a repo with that did"
      in
      let%lwt {db= user_db; _} = Repository.load did in
      let%lwt _, commit =
        match%lwt User_store.get_commit user_db with
        | Some c ->
            Lwt.return c
        | None ->
            failwith ("failed to retrieve commit for " ^ actor.did)
      in
      let active, status, rev =
        match actor.deactivated_at with
        | Some _ ->
            (false, Some "deactivated", None)
        | None ->
            (true, None, Some commit.rev)
      in
      Dream.json @@ Yojson.Safe.to_string
      @@ output_to_yojson {did; active; status; rev} )
