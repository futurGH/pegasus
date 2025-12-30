open Lexicons.Com_atproto_repo_applyWrites.Main

let calc_write_points writes =
  List.fold_left
    (fun acc write ->
      acc + match write with Create _ -> 3 | Update _ -> 2 | Delete _ -> 1 )
    0 writes

let handler =
  Xrpc.handler ~auth:Authorization (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req input_of_yojson in
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      (* apply rate limits after parsing body so we can count points accurately *)
      let points = calc_write_points input.writes in
      let _ =
        Xrpc.consume_shared_rate_limit ~name:"repo-write-hour" ~key:did ~points
      in
      let _ =
        Xrpc.consume_shared_rate_limit ~name:"repo-write-day" ~key:did ~points
      in
      (* check oauth scopes for each write operation *)
      List.iter
        (fun write ->
          match write with
          | Create {collection; _} ->
              Auth.assert_repo_scope ctx.auth ~collection
                ~action:Oauth.Scopes.Create
          | Update {collection; _} ->
              Auth.assert_repo_scope ctx.auth ~collection
                ~action:Oauth.Scopes.Update
          | Delete {collection; _} ->
              Auth.assert_repo_scope ctx.auth ~collection
                ~action:Oauth.Scopes.Delete )
        input.writes ;
      let%lwt repo = Repository.load did in
      let repo_writes =
        List.map
          (fun w ->
            w |> writes_item_to_yojson |> Repository.repo_write_of_yojson
            |> Result.get_ok )
          input.writes
      in
      let%lwt {commit= commit_cid, {rev; _}; results= aw_results} =
        Repository.apply_writes repo repo_writes
          (Option.map Cid.as_cid input.swap_commit)
      in
      let results =
        Option.some
        @@ List.map
             (fun r ->
               r |> Repository.apply_writes_result_to_yojson
               |> results_item_of_yojson |> Result.get_ok )
             aw_results
      in
      Dream.json @@ Yojson.Safe.to_string
      @@ output_to_yojson
           {commit= Some {cid= Cid.to_string commit_cid; rev}; results} )
