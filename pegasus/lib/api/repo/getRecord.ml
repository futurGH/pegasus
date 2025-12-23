type query =
  { repo: string
  ; collection: string
  ; rkey: string
  ; cid: string option [@default None] }
[@@deriving yojson {strict= false}]

type response = {uri: string; cid: string; value: Mist.Lex.repo_record}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun ctx ->
      let input = Xrpc.parse_query ctx.req query_of_yojson in
      try%lwt
        let%lwt input_did = Xrpc.resolve_repo_did ctx input.repo in
        let uri =
          Util.make_at_uri ~repo:input_did ~collection:input.collection
            ~rkey:input.rkey ~fragment:None
        in
        let%lwt repo = Repository.load ~ensure_active:true input_did in
        let path = input.collection ^ "/" ^ input.rkey in
        match%lwt Repository.get_record repo path with
        | Some {cid; value; _}
          when input.cid = None || input.cid = Some (Cid.to_string cid) ->
            Dream.json @@ Yojson.Safe.to_string
            @@ response_to_yojson {uri; cid= Cid.to_string cid; value}
        | _ ->
            Errors.internal_error ~name:"RecordNotFound"
              ~msg:("could not find record " ^ uri)
              ()
      with _ -> (
        let%lwt input_did =
          if String.starts_with ~prefix:"did:" input.repo then
            Lwt.return input.repo
          else
            match%lwt Id_resolver.Handle.resolve input.repo with
            | Ok did ->
                Lwt.return did
            | Error _ ->
                Errors.invalid_request "failed to resolve repo"
        in
        let%lwt pds =
          match%lwt Id_resolver.Did.resolve input_did with
          | Ok doc -> (
              Lwt.return
              @@
              match Id_resolver.Did.Document.get_service doc "#atproto_pds" with
              | Some service ->
                  service
              | None ->
                  Errors.invalid_request "failed to resolve repo pds" )
          | Error _ ->
              Errors.invalid_request "failed to resolve repo did document"
        in
        if pds = Env.host_endpoint then
          Errors.internal_error ~name:"RecordNotFound"
            ~msg:("could not resolve user " ^ input.repo)
            () ;
        let get_uri = Uri.of_string pds in
        let get_uri =
          Uri.with_path get_uri "/xrpc/com.atproto.repo.getRecord"
        in
        let get_uri = Uri.with_query get_uri (Util.copy_query ctx.req) in
        let%lwt res, body =
          Util.http_get get_uri
            ~headers:(Cohttp.Header.of_list [("Accept", "application/json")])
        in
        match res.status with
        | `OK ->
            let%lwt json = Cohttp_lwt.Body.to_string body in
            let%lwt () = Cohttp_lwt.Body.drain_body body in
            Dream.json json
        | _ ->
            let%lwt () = Cohttp_lwt.Body.drain_body body in
            Errors.internal_error ~name:"RecordNotFound"
              ~msg:
                ( "could not find record "
                ^ Util.make_at_uri ~repo:input.repo ~collection:input.collection
                    ~rkey:input.rkey ~fragment:None )
              () ) )
