type request = {handle: string} [@@deriving yojson]

let update_handle ~did ~handle db =
  match Util.validate_handle handle with
  | Error e ->
      Lwt.return_error e
  | Ok () -> (
    match%lwt Data_store.get_actor_by_identifier handle db with
    | Some _ ->
        Lwt.return_error "handle already in use"
    | None ->
        let%lwt () = Data_store.update_actor_handle ~did ~handle db in
        let%lwt plc_result =
          if String.starts_with ~prefix:"did:plc:" did then
            match%lwt Plc.get_audit_log did with
            | Error e ->
                Lwt.return_error ("failed to fetch did doc: " ^ e)
            | Ok log -> (
                let latest = List.rev log |> List.hd in
                let aka =
                  match
                    List.mem ("at://" ^ handle) latest.operation.also_known_as
                  with
                  | true ->
                      latest.operation.also_known_as
                  | false ->
                      ("at://" ^ handle) :: latest.operation.also_known_as
                in
                let signed =
                  Plc.sign_operation Env.rotation_key
                    (Operation
                       { type'= "plc_operation"
                       ; prev= Some latest.cid
                       ; also_known_as= aka
                       ; rotation_keys= latest.operation.rotation_keys
                       ; verification_methods=
                           latest.operation.verification_methods
                       ; services= latest.operation.services } )
                in
                match%lwt Plc.submit_operation did signed with
                | Ok _ ->
                    Lwt.return_ok ()
                | Error (status, msg) ->
                    Lwt.return_error
                      (Printf.sprintf "failed to submit plc operation: %d %s"
                         status msg ) )
          else Lwt.return_ok ()
        in
        match plc_result with
        | Error e ->
            Lwt.return_error e
        | Ok () ->
            let () = Ttl_cache.String_cache.remove Id_resolver.Did.cache did in
            let%lwt _ = Sequencer.sequence_identity db ~did ~handle () in
            Lwt.return_ok () )

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      Auth.assert_identity_scope auth ~attr:Oauth.Scopes.Handle ;
      let did = Auth.get_authed_did_exn auth in
      let%lwt {handle} = Xrpc.parse_body req request_of_yojson in
      match%lwt update_handle ~did ~handle db with
      | Ok () ->
          Dream.empty `OK
      | Error e ->
          Dream.error (fun log -> log ~request:req "%s" e) ;
          Errors.invalid_request ~name:"InvalidHandle" e )
