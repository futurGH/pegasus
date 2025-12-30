open Lexicons.Com_atproto_admin_deleteAccount.Main

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {did} = Xrpc.parse_body req input_of_yojson in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.invalid_request "account not found"
      | Some _ ->
          let%lwt _ = Server.DeleteAccount.delete_account ~did db in
          Dream.empty `OK )
