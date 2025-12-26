type query = {did: string; cid: string} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun ctx ->
      let {did; cid} = Xrpc.parse_query ctx.req query_of_yojson in
      let cid_parsed = Cid.as_cid cid in
      let%lwt {db; _} = Repository.load did ~ensure_active:true in
      let%lwt blob_meta = User_store.get_blob_metadata db cid_parsed in
      match blob_meta with
      | None ->
          Errors.internal_error ~msg:"blob not found" ()
      | Some {storage= Blob_store.S3; _} -> (
        match Blob_store.cdn_redirect_url ~did ~cid with
        | Some url ->
            (* redirect to CDN *)
            Lwt.return
            @@ Dream.response ~status:`Found ~headers:[("Location", url)] ""
        | None ->
            (* no CDN, proxy from S3 *)
            let%lwt blob =
              match%lwt User_store.get_blob db cid_parsed with
              | Some blob ->
                  Lwt.return blob
              | None ->
                  Errors.internal_error ~msg:"blob data not found" ()
            in
            Lwt.return
            @@ Dream.response
                 ~headers:[("Content-Type", blob.mimetype)]
                 (Bytes.to_string blob.data) )
      | Some _ ->
          (* return from local storage *)
          let%lwt blob =
            match%lwt User_store.get_blob db cid_parsed with
            | Some blob ->
                Lwt.return blob
            | None ->
                Errors.internal_error ~msg:"blob not found" ()
          in
          Lwt.return
          @@ Dream.response
               ~headers:[("Content-Type", blob.mimetype)]
               (Bytes.to_string blob.data) )
