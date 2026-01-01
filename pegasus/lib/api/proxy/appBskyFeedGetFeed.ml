type query =
  { feed: string
  ; limit: int option [@default None]
  ; cursor: string option [@default None] }
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Authorization (fun ctx ->
      let input = Xrpc.parse_query ctx.req query_of_yojson in
      match Util.parse_at_uri input.feed with
      | None ->
          Errors.invalid_request ("invalid feed URI " ^ input.feed)
      | Some {repo; collection; rkey; _} -> (
        match%lwt Id_resolver.Did.resolve repo with
        | Error e ->
            Errors.internal_error
              ~msg:("failed to resolve feed publisher " ^ repo ^ ": " ^ e)
              ()
        | Ok did_doc -> (
            let pds_host =
              match
                Id_resolver.Did.Document.get_service did_doc "#atproto_pds"
              with
              | Some endpoint ->
                  endpoint
              | None ->
                  Errors.invalid_request "feed publisher has no PDS endpoint"
            in
            try%lwt
              let client = Hermes.make_client ~service:pds_host () in
              let%lwt {value= record; _} =
                Lexicons.([%xrpc get "com.atproto.repo.getRecord"])
                  ~repo ~collection ~rkey client
              in
              let feed_generator_did =
                Yojson.Safe.Util.(record |> member "did" |> to_string_option)
              in
              match feed_generator_did with
              | None ->
                  Errors.invalid_request
                    "feed generator record missing 'did' field"
              | Some fg_did -> (
                match Dream.header ctx.req "atproto-proxy" with
                | Some appview ->
                    Auth.assert_rpc_scope ctx.auth ~lxm:"app.bsky.feed.getFeed"
                      ~aud:appview ;
                    Xrpc.service_proxy ctx ~aud:fg_did
                      ~lxm:"app.bsky.feed.getFeedSkeleton"
                | None ->
                    Errors.invalid_request "missing proxy header" )
            with e ->
              Log.err (fun log ->
                  log "failed to fetch feed generator record: %s"
                    (Printexc.to_string e) ) ;
              Errors.internal_error ~msg:"failed to fetch feed generator record"
                () ) ) )
