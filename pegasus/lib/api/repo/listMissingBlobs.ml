open Lexicons.Com_atproto_repo_listMissingBlobs
open Main

let handler =
  Xrpc.handler (fun ctx ->
      let {limit; cursor} = Xrpc.parse_query ctx.req params_of_yojson in
      let limit =
        match limit with
        | Some limit when limit > 0 && limit <= 1000 ->
            limit
        | _ ->
            500
      in
      let did = Auth.get_authed_did_exn ctx.auth in
      let%lwt {db; _} = Repository.load ~ensure_active:true did in
      let%lwt blobs = User_store.list_missing_blobs ~limit ?cursor db in
      let next_cursor = ref None in
      let blobs =
        List.map
          (fun (path, cid) ->
            let cid = Cid.to_string cid in
            next_cursor := Some cid ;
            {cid; record_uri= "at://" ^ did ^ "/" ^ path} )
          blobs
      in
      {cursor= !next_cursor; blobs}
      |> output_to_yojson |> Yojson.Safe.to_string |> Dream.json )
