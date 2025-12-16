type query = {did: string} [@@deriving yojson {strict= false}]

let rec stream_to_seq stream () =
  let%lwt chunk = Dream.read stream in
  match chunk with
  | None ->
      Lwt.return Lwt_seq.Nil
  | Some data ->
      Lwt.return (Lwt_seq.Cons (Bytes.of_string data, stream_to_seq stream))

let handler =
  Xrpc.handler ~auth:Authorization (fun ctx ->
      let did = Auth.get_authed_did_exn ctx.auth in
      let bytes_stream = Dream.body_stream ctx.req in
      let car_stream = stream_to_seq bytes_stream in
      let%lwt repo =
        Repository.load did ~ds:ctx.db ~ensure_active:true ~write:true
      in
      let%lwt result = Repository.import_car repo car_stream in
      match result with
      | Ok _ ->
          Dream.empty `OK
      | Error e ->
          Errors.internal_error ~msg:(Printexc.to_string e) () )
