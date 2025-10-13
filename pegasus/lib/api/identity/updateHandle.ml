type request = {handle: string} [@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      let%lwt body = Dream.body req in
      let handle =
        match Yojson.Safe.from_string body |> request_of_yojson with
        | Ok {handle} ->
            handle
        | Error _ ->
            Errors.invalid_request "invalid request body"
      in
      match Util.validate_handle handle with
      | Error e ->
          raise e
      | Ok () -> (
          match%lwt Data_store.get_actor_by_identifier handle db with
          | Some _ ->
              Errors.invalid_request ~name:"InvalidHandle"
                "handle already in use"
          | None ->
              let%lwt () = Data_store.update_actor_handle ~did ~handle db in
              let%lwt _ =
                if String.starts_with ~prefix:"did:plc:" did then
                  match%lwt Plc.get_audit_log did with
                  | Error e ->
                      Dream.error (fun log -> log ~request:req "%s" e) ;
                      Errors.internal_error ~msg:"failed to fetch did doc" ()
                  | Ok log -> (
                      let latest = List.rev log |> List.hd in
                      let aka =
                        match
                          List.mem ("at://" ^ handle)
                            latest.operation.also_known_as
                        with
                        | true ->
                            latest.operation.also_known_as
                        | false ->
                            ("at://" ^ handle) :: latest.operation.also_known_as
                      in
                      let%lwt signing_key =
                        match%lwt Data_store.get_actor_by_identifier did db with
                        | Some {signing_key; _} ->
                            Lwt.return @@ Kleidos.parse_multikey_str signing_key
                        | _ ->
                            Errors.internal_error ()
                      in
                      let signed =
                        Plc.sign_operation signing_key
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
                          Lwt.return_unit
                      | Error (status, msg) ->
                          Dream.error (fun log ->
                              log ~request:req "%d %s" status msg ) ;
                          Errors.internal_error
                            ~msg:"failed to submit plc operation" () )
                else Lwt.return_unit
              in
              let () =
                Ttl_cache.String_cache.remove Id_resolver.Did.cache did
              in
              let%lwt _ = Sequencer.sequence_identity db ~did ~handle () in
              Dream.empty `OK ) )
