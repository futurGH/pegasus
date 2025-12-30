open Lexicons.Com_atproto_identity_submitPlcOperation.Main

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      Auth.assert_identity_scope auth ~attr:Oauth.Scopes.Any ;
      if not (String.starts_with ~prefix:"did:plc:" did) then
        Errors.invalid_request "this method is only for did:plc identities" ;
      let%lwt {operation= op} = Xrpc.parse_body req input_of_yojson in
      let op =
        try Result.get_ok @@ Plc.signed_operation_op_of_yojson op
        with _ -> Errors.invalid_request "invalid request body"
      in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor -> (
        match
          Plc.validate_operation ~handle:actor.handle
            ~signing_key:actor.signing_key (Operation op)
        with
        | Ok () -> (
          match%lwt Plc.submit_operation did (Operation op) with
          | Ok () ->
              let%lwt _ = Sequencer.sequence_identity db ~did () in
              let%lwt _ = Id_resolver.Did.resolve did in
              Dream.empty `OK
          | Error (status, msg) ->
              Errors.internal_error
                ~msg:
                  ( "failed to submit plc operation: " ^ Int.to_string status
                  ^ " " ^ msg )
                () )
        | Error e ->
            Errors.invalid_request e ) )
