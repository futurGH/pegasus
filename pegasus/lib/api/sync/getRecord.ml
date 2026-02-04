module Mst = Mist.Mst.Make (User_store)
open Lexicons.Com.Atproto.Sync.GetRecord.Main

let handler =
  Xrpc.handler (fun ctx ->
      let {did; collection; rkey} = Xrpc.parse_query ctx.req params_of_yojson in
      let path = collection ^ "/" ^ rkey in
      let%lwt repo = Repository.load did ~ensure_active:true in
      let commit_cid, commit_signed = Option.get repo.commit in
      let commit_block =
        commit_signed |> User_store.Types.signed_commit_to_yojson
        |> Dag_cbor.encode_yojson
      in
      let mst_root = commit_signed.data in
      let%lwt record = Repository.get_record repo path in
      let%lwt record_blocks =
        match record with
        | Some record ->
            let record_block =
              Mist.Lex.repo_record_to_cbor_block record.value
            in
            let%lwt blocks =
              Mst.proof_for_key
                {blockstore= repo.db; root= mst_root}
                mst_root path
            in
            let blocks_stream =
              Repository.Block_map.entries blocks |> Lwt_seq.of_list
            in
            Lwt.return @@ Lwt_seq.cons record_block blocks_stream
        | None ->
            Lwt.return Lwt_seq.empty
      in
      let car_stream =
        Lwt_seq.cons (commit_cid, commit_block) record_blocks
        |> Car.blocks_to_stream commit_cid
      in
      Dream.stream
        ~headers:[("Content-Type", "application/vnd.ipld.car")]
        (fun res_stream ->
          Lwt_seq.iter_s
            (fun chunk -> Dream.write res_stream (Bytes.to_string chunk))
            car_stream ) )
