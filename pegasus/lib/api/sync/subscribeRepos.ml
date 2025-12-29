let handler =
  Xrpc.handler (fun {req; db; _} ->
      Dream.websocket (fun ws ->
          let cursor =
            match Dream.query req "cursor" with
            | Some s ->
                Some (max 0 (Option.value (int_of_string_opt s) ~default:0))
            | None ->
                None
          in
          let closed = ref false in
          let send (bytes : bytes) =
            if !closed then Lwt.fail Exit
            else
              Lwt.catch
                (fun () ->
                  Lwt_unix.with_timeout 30.0 (fun () ->
                      Dream.send ~text_or_binary:`Binary ws
                        (Bytes.unsafe_to_string bytes) ) )
                (fun _ ->
                  closed := true ;
                  Lwt.fail Exit )
          in
          let stream () =
            Sequencer.Live.stream_with_backfill ~conn:db ~cursor ~send
          in
          let wait_for_close () =
            let rec loop () =
              match%lwt Dream.receive ws with
              | Some _ ->
                  loop ()
              | None ->
                  closed := true ;
                  Lwt.fail Exit
            in
            loop ()
          in
          Lwt.catch
            (fun () -> Lwt.pick [stream (); wait_for_close ()])
            (fun _exn -> Lwt.return_unit) ) )
