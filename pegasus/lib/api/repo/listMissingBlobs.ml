type query =
  {limit: int option [@default None]; cursor: string option [@default None]}
[@@deriving yojson {strict= false}]

type response =
  {cursor: string option [@default None]; blobs: response_blob list}
[@@deriving yojson {strict= false}]

and response_blob = {cid: string; record_uri: string [@key "recordUri"]}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun ctx ->
      let {limit; cursor} = Xrpc.parse_query ctx.req query_of_yojson in
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
      |> response_to_yojson |> Yojson.Safe.to_string |> Dream.json )
