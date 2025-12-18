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
                Option.bind
                  (Id_resolver.Did.Document.get_service did_doc "#atproto_pds")
                  (fun s -> s |> Uri.of_string |> Uri.host )
              with
              | Some endpoint ->
                  endpoint
              | None ->
                  Errors.invalid_request "feed publisher has no PDS endpoint"
            in
            let get_record_uri =
              Uri.make ~scheme:"https" ~host:pds_host
                ~path:"/xrpc/com.atproto.repo.getRecord"
                ~query:
                  [ ("repo", [repo])
                  ; ("collection", [collection])
                  ; ("rkey", [rkey]) ]
                ()
            in
            let%lwt res, body = Util.http_get get_record_uri in
            match res.status with
            | `OK -> (
                let%lwt body_str = Cohttp_lwt.Body.to_string body in
                let json = Yojson.Safe.from_string body_str in
                let value = Yojson.Safe.Util.(json |> member "value") in
                let feed_generator_did =
                  Yojson.Safe.Util.(value |> member "did" |> to_string_option)
                in
                match feed_generator_did with
                | None ->
                    Errors.invalid_request
                      "feed generator record missing 'did' field"
                | Some fg_did -> (
                  match Dream.header ctx.req "atproto-proxy" with
                  | Some appview ->
                      Auth.assert_rpc_scope ctx.auth
                        ~lxm:"app.bsky.feed.getFeed" ~aud:appview ;
                      Xrpc.service_proxy ctx ~aud:fg_did
                        ~lxm:"app.bsky.feed.getFeedSkeleton"
                  | None ->
                      Errors.invalid_request "missing proxy header" ) )
            | _ ->
                let%lwt () = Cohttp_lwt.Body.drain_body body in
                Errors.internal_error
                  ~msg:"failed to fetch feed generator record" () ) ) )
