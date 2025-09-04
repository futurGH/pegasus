let handler =
  Xrpc.handler (fun {req; db; _} ->
      Dream.websocket (fun ws ->
          let cursor =
            match Dream.query req "cursor" with
            | Some s ->
                Option.value (int_of_string_opt s) ~default:0
            | None ->
                0
          in
          let send (bytes : bytes) =
            Dream.send ~text_or_binary:`Binary ws (Bytes.unsafe_to_string bytes)
          in
          Lwt.catch
            (fun () ->
              Sequencer.Live.stream_with_backfill ~conn:db ~cursor ~send )
            (fun _exn -> Lwt.return_unit) ) )
