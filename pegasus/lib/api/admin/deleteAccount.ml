type request = {did: string} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {did} = Xrpc.parse_body req request_of_yojson in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.invalid_request "account not found"
      | Some _ ->
          let%lwt _ = Server.DeleteAccount.delete_account ~did db in
          Dream.empty `OK )
