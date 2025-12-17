type request = {delete_after: string option [@key "deleteAfter"] [@default None]}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      (* TODO: handle delete_after *)
      let%lwt _req = Xrpc.parse_body req request_of_yojson in
      let%lwt () = Data_store.deactivate_actor did db in
      let%lwt _ =
        Sequencer.sequence_account db ~did ~active:false ~status:`Deactivated ()
      in
      Dream.empty `OK )
