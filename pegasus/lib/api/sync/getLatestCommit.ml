type query = {did: string} [@@deriving yojson {strict= false}]

type response = {cid: string; rev: string} [@@deriving yojson]

let handler =
  Xrpc.handler (fun ctx ->
      let {did} : query = Xrpc.parse_query ctx.req query_of_yojson in
      match%lwt
        Repository.load did ~ensure_active:true ~ds:ctx.db
      with
      | {commit= Some (cid, {rev; _}); _} ->
          let cid = Cid.to_string cid in
          Dream.json @@ Yojson.Safe.to_string @@ response_to_yojson {cid; rev}
      | _ ->
          failwith ("couldn't resolve commit for " ^ did) )
