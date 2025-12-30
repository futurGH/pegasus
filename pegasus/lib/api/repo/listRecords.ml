open Lexicons.Com_atproto_repo_listRecords
open Main

let handler =
  Xrpc.handler (fun ctx ->
      let input = Xrpc.parse_query ctx.req params_of_yojson in
      let limit =
        match input.limit with
        | Some limit when limit > 0 && limit <= 100 ->
            limit
        | _ ->
            100
      in
      let%lwt input_did = Xrpc.resolve_repo_did ctx input.repo in
      let%lwt {db; _} = Repository.load ~ensure_active:true input_did in
      let%lwt results =
        User_store.list_records db ~limit ?cursor:input.cursor
          ?reverse:input.reverse input.collection
      in
      let cursor, results_rev =
        List.fold_left
          (fun (_cursor, results_rev) (record : User_store.Types.record) ->
            let uri = "at://" ^ input_did ^ "/" ^ record.path in
            ( record.since
            , { uri
              ; cid= Cid.to_string record.cid
              ; value= Repository.Lex.repo_record_to_yojson record.value }
              :: results_rev ) )
          ("", []) results
      in
      let cursor = if List.length results = limit then Some cursor else None in
      Dream.json @@ Yojson.Safe.to_string
      @@ output_to_yojson {cursor; records= List.rev results_rev} )
