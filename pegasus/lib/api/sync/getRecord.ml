type query = {did: string; collection: string; rkey: string} [@@deriving yojson]

type response = Mist.Lex.repo_record [@@deriving yojson]

let handler =
  Xrpc.handler (fun ctx ->
      let {did; collection; rkey} : query =
        Xrpc.parse_query ctx.req query_of_yojson
      in
      let path = collection ^ "/" ^ rkey in
      let%lwt {db; _} = Repository.load did ~write:false ~ds:ctx.db in
      match%lwt User_store.get_record_by_path db path with
      | Some {value; _} ->
          Dream.json @@ Yojson.Safe.to_string @@ response_to_yojson value
      | None ->
          Errors.invalid_request ~name:"RecordNotFound" "record not found" )
