type query = {repo: string; collection: string; rkey: string; cid: string option}
[@@deriving yojson]

type response = {uri: string; cid: string; value: Mist.Lex.repo_record}
[@@deriving yojson]

let handler =
  Xrpc.handler (fun ctx ->
      let%lwt input = Xrpc.parse_query ctx.req query_of_yojson in
      let%lwt input_did =
        if String.starts_with ~prefix:"did:" input.repo then
          Lwt.return input.repo
        else
          match%lwt Data_store.get_actor_by_identifier input.repo ctx.db with
          | Some {did; _} ->
              Lwt.return did
          | None ->
              Errors.invalid_request "target repository not found"
      in
      let%lwt repo = Repository.load input_did in
      let path = input.collection ^ "/" ^ input.rkey in
      let uri = "at://" ^ input_did ^ "/" ^ path in
      match%lwt Repository.get_record repo path with
      | Some {cid; value; _}
        when input.cid = None || input.cid = Some (Cid.to_string cid) ->
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson {uri; cid= Cid.to_string cid; value}
      | _ ->
          Errors.internal_error ~name:"RecordNotFound"
            ~msg:("could not find record " ^ uri)
            () )
