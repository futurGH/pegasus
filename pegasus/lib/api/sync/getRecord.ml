module Mst = Mist.Mst.Make (User_store)

type query = {did: string; collection: string; rkey: string}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun ctx ->
      let {did; collection; rkey} : query =
        Xrpc.parse_query ctx.req query_of_yojson
      in
      let path = collection ^ "/" ^ rkey in
      let%lwt repo =
        Repository.load did ~ensure_active:true ~ds:ctx.db
      in
      match%lwt Repository.get_record repo path with
      | None ->
          Printf.ksprintf
            (Errors.not_found ?name:None)
            "record %s not found"
            ("at://" ^ did ^ "/" ^ path)
      | Some record ->
          let record_block = Mist.Lex.repo_record_to_cbor_block record.value in
          let commit_cid, commit_signed = Option.get repo.commit in
          let commit_block =
            commit_signed |> User_store.Types.signed_commit_to_yojson
            |> Dag_cbor.encode_yojson
          in
          let mst_root = commit_signed.data in
          let%lwt blocks =
            Mst.proof_for_key
              {blockstore= repo.db; root= mst_root}
              mst_root path
          in
          let blocks_stream =
            Repository.Block_map.entries blocks |> Lwt_seq.of_list
          in
          let car_stream =
            Lwt_seq.cons (commit_cid, commit_block)
            @@ Lwt_seq.cons record_block blocks_stream
            |> Car.blocks_to_stream commit_cid
          in
          Dream.stream
            ~headers:[("Content-Type", "application/vnd.ipld.car")]
            (fun res_stream ->
              Lwt_seq.iter_s
                (fun chunk -> Dream.write res_stream (Bytes.to_string chunk))
                car_stream ) )
