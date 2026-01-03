open Lexicons.Com.Atproto.Admin.UpdateAccountEmail.Main

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {account; email} = Xrpc.parse_body req input_of_yojson in
      match%lwt Data_store.get_actor_by_identifier account db with
      | None ->
          Errors.invalid_request "account not found"
      | Some actor ->
          let email = String.lowercase_ascii email in
          let%lwt () = Data_store.update_email ~did:actor.did ~email db in
          Dream.empty `OK )
