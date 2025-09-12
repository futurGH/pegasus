type query = {did: string; cid: string} [@@deriving yojson]

let handler =
  Xrpc.handler (fun ctx ->
      let {did; cid} = Xrpc.parse_query ctx.req query_of_yojson in
      let cid = Cid.as_cid cid in
      let%lwt {db; _} = Repository.load did ~write:false ~ds:ctx.db in
      let%lwt blob =
        match%lwt User_store.get_blob db cid with
        | Some blob ->
            Lwt.return blob
        | None ->
            Errors.internal_error ~msg:"blob not found" ()
      in
      Lwt.return
      @@ Dream.response
           ~headers:[("Content-Type", blob.mimetype)]
           (Bytes.to_string blob.data) )
