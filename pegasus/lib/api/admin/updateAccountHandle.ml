open Lexicons.Com.Atproto.Admin.UpdateAccountHandle.Main

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {did; handle} = Xrpc.parse_body req input_of_yojson in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.invalid_request "account not found"
      | Some _ -> (
        match%lwt Identity_util.update_handle ~did ~handle db with
        | Ok () ->
            Dream.empty `OK
        | Error e ->
            Errors.invalid_request ~name:"InvalidHandle"
              (Identity_util.update_handle_error_to_string e) ) )
