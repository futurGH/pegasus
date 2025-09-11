type query =
  { repo: string
  ; collection: string
  ; limit: int option
  ; cursor: string option
  ; reverse: bool option }
[@@deriving yojson]

type response = {cursor: string option; records: response_record list}
[@@deriving yojson]

and response_record = {uri: string; cid: string; value: Mist.Lex.repo_record}
[@@deriving yojson]

let handler =
  Xrpc.handler (fun ctx ->
      let input = Xrpc.parse_query ctx.req query_of_yojson in
      let limit =
        match input.limit with
        | Some limit when limit > 0 && limit <= 100 ->
            limit
        | _ ->
            100
      in
      let%lwt input_did = Xrpc.resolve_repo_did ctx input.repo in
      let%lwt db = User_store.connect input_did in
      let%lwt results =
        User_store.list_records db ~limit ?cursor:input.cursor
          ?reverse:input.reverse input.collection
      in
      let cursor, results_rev =
        List.fold_left
          (fun (_cursor, results_rev) (record : User_store.Types.record) ->
            let uri = "at://" ^ input_did ^ "/" ^ record.path in
            ( record.since
            , {uri; cid= Cid.to_string record.cid; value= record.value}
              :: results_rev ) )
          ("", []) results
      in
      Dream.json @@ Yojson.Safe.to_string
      @@ response_to_yojson {cursor= Some cursor; records= List.rev results_rev} )
