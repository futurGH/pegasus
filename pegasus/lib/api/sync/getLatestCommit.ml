open Lexicons.Com_atproto_sync_getLatestCommit.Main

let handler =
  Xrpc.handler (fun ctx ->
      let {did} = Xrpc.parse_query ctx.req params_of_yojson in
      match%lwt Repository.load did ~ensure_active:true with
      | {commit= Some (cid, {rev; _}); _} ->
          let cid = Cid.to_string cid in
          Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson {cid; rev}
      | _ ->
          failwith ("couldn't resolve commit for " ^ did) )
