open Lexicons.Com.Atproto.Repo.GetRecord.Main

let handler =
  Xrpc.handler (fun ctx ->
      let input = Xrpc.parse_query ctx.req params_of_yojson in
      let%lwt input_did =
        Lwt_result.catch @@ fun () -> Xrpc.resolve_repo_did ctx input.repo
      in
      match input_did with
      | Ok input_did -> (
          let uri =
            Util.Syntax.make_at_uri ~repo:input_did ~collection:input.collection
              ~rkey:input.rkey ~fragment:None
          in
          let%lwt repo = Repository.load ~ensure_active:true input_did in
          let path = input.collection ^ "/" ^ input.rkey in
          match%lwt Repository.get_record repo path with
          | Some {cid; value; _}
            when input.cid = None || input.cid = Some (Cid.to_string cid) ->
              Dream.json @@ Yojson.Safe.to_string
              @@ output_to_yojson
                   { uri
                   ; cid= Some (Cid.to_string cid)
                   ; value= Repository.Lex.repo_record_to_yojson value }
          | _ ->
              Errors.internal_error ~name:"RecordNotFound"
                ~msg:("could not find record " ^ uri)
                () )
      | Error _ -> (
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
                match
                  Id_resolver.Did.Document.get_service doc "#atproto_pds"
                with
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
          let client = Hermes.make_client ~service:pds () in
          try%lwt
            let%lwt record =
              Lexicons.([%xrpc get "com.atproto.repo.getRecord"])
                ~repo:input_did ~collection:input.collection ~rkey:input.rkey
                client
            in
            record |> output_to_yojson |> Yojson.Safe.to_string |> Dream.json
          with _ ->
            Errors.internal_error ~name:"RecordNotFound"
              ~msg:
                ( "could not find record "
                ^ Util.Syntax.make_at_uri ~repo:input.repo ~collection:input.collection
                    ~rkey:input.rkey ~fragment:None )
              () ) )
