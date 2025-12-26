type query = {repo: string} [@@deriving yojson {strict= false}]

type response =
  { handle: string
  ; did: string
  ; did_doc: Id_resolver.Did.Document.t [@key "didDoc"]
  ; collections: string list
  ; handle_is_correct: bool [@key "handleIsCorrect"] }
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun ctx ->
      let input = Xrpc.parse_query ctx.req query_of_yojson in
      let%lwt did = Xrpc.resolve_repo_did ctx input.repo in
      let%lwt did_doc =
        match%lwt Id_resolver.Did.resolve did with
        | Ok did_doc ->
            Lwt.return did_doc
        | Error err ->
            failwith err
      in
      let handle =
        Scanf.sscanf
          (did_doc.also_known_as |> Option.get |> List.hd)
          "at://%s"
          (fun h -> h)
      in
      let%lwt repo = Repository.load did in
      let%lwt collections = Repository.list_collections repo in
      Dream.json @@ Yojson.Safe.to_string
      @@ response_to_yojson
           { handle
           ; did
           ; did_doc
           ; collections
           ; handle_is_correct= true (* what am I, a cop? *) } )
