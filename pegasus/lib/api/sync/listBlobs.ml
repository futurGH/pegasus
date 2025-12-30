open Lexicons.Com_atproto_sync_listBlobs.Main

let handler =
  Xrpc.handler (fun ctx ->
      let {did; since; limit; cursor} =
        Xrpc.parse_query ctx.req params_of_yojson
      in
      let cursor = Option.value ~default:"" cursor in
      let limit =
        match limit with
        | Some limit when limit > 0 && limit <= 1000 ->
            limit
        | _ ->
            1000
      in
      let%lwt {db; _} = Repository.load did ~ensure_active:true in
      let%lwt cids = User_store.list_blobs db ~limit ~cursor ?since in
      let cids = List.map Cid.to_string cids in
      let cursor =
        if List.length cids = limit then Mist.Util.last cids else None
      in
      Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson {cursor; cids} )
