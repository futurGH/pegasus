type response = {blob: Mist.Blob_ref.typed_json_ref} [@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Authorization (fun ctx ->
      let did = Auth.get_authed_did_exn ctx.auth in
      let mime_type =
        Option.value ~default:"application/octet-stream"
          (Dream.header ctx.req "Content-Type")
      in
      Auth.assert_blob_scope ctx.auth ~mime:mime_type ;
      let%lwt data = Dream.body ctx.req |> Lwt.map Bytes.of_string in
      let size = Int64.of_int @@ Bytes.length data in
      let cid = Cid.create Raw data in
      let%lwt user_db = User_store.connect did in
      let%lwt _ = User_store.put_blob user_db cid mime_type data in
      Dream.json @@ Yojson.Safe.to_string
      @@ response_to_yojson {blob= {type'= "blob"; ref= cid; mime_type; size}} )
