module Mst = Mist.Mst.Make (User_store)

type query = {did: string; collection: string; rkey: string} [@@deriving yojson]

let handler =
  Xrpc.handler (fun ctx ->
      let {did; collection; rkey} : query =
        Xrpc.parse_query ctx.req query_of_yojson
      in
      let path = collection ^ "/" ^ rkey in
      let%lwt {db; commit; _} =
        Repository.load did ~ensure_active:true ~write:false ~ds:ctx.db
      in
      let commit_cid, commit_signed = Option.get commit in
      let commit_block =
        commit_signed |> User_store.Types.signed_commit_to_yojson
        |> Dag_cbor.encode_yojson
      in
      let mst_root = commit_signed.data in
      let%lwt blocks =
        Mst.proof_for_key {blockstore= db; root= mst_root} mst_root path
      in
      let blocks_stream =
        Repository.BlockMap.entries blocks |> Lwt_seq.of_list
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
            car_stream ) )
