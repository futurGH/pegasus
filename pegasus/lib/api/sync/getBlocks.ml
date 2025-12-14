type query = {did: string; cids: string list}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun ctx ->
      let {did; cids} : query = Xrpc.parse_query ctx.req query_of_yojson in
      let%lwt {db; commit; _} =
        Repository.load did ~ensure_active:true ~write:false ~ds:ctx.db
      in
      let commit_cid, commit_signed = Option.get commit in
      let commit_block =
        commit_signed |> User_store.Types.signed_commit_to_yojson
        |> Dag_cbor.encode_yojson
      in
      let cids = List.map Cid.as_cid cids in
      match%lwt User_store.get_blocks db cids with
      | {blocks; missing= []} ->
          let blocks_stream =
            Repository.Block_map.entries blocks |> Lwt_seq.of_list
          in
          let car_stream =
            Lwt_seq.cons (commit_cid, commit_block) blocks_stream
            |> Car.blocks_to_stream commit_cid
          in
          Dream.stream
            ~headers:[("Content-Type", "application/vnd.ipld.car")]
            (fun res_stream ->
              Lwt_seq.iter_s
                (fun chunk -> Dream.write res_stream (Bytes.to_string chunk))
                car_stream )
      | {missing; _} ->
          let missing_cids =
            List.map Cid.to_string missing |> String.concat ", "
          in
          Errors.invalid_request ~name:"BlockNotFound"
            ("missing the following blocks: " ^ missing_cids) )
