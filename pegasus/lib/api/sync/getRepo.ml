type query = {did: string} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler (fun ctx ->
      let {did} : query = Xrpc.parse_query ctx.req query_of_yojson in
      let%lwt repo = Repository.load did ~ensure_active:true ~write:false in
      let%lwt car_stream = Repository.export_car repo in
      Dream.stream
        ~headers:[("Content-Type", "application/vnd.ipld.car")]
        (fun res_stream ->
          let%lwt () =
            Lwt_seq.iter_s
              (fun chunk -> Dream.write res_stream (Bytes.to_string chunk))
              car_stream
          in
          Dream.close res_stream ) )
