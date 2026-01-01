open Lexicons.Com_atproto_identity_updateHandle.Main

type update_handle_error =
  | InvalidFormat of string
  | HandleTaken
  | TooShort of string
  | TooLong of string
  | InternalServerError of string

let update_handle_error_to_string = function
  | InvalidFormat m | TooShort m | TooLong m ->
      "handle " ^ m
  | HandleTaken ->
      "handle already taken"
  | InternalServerError msg ->
      msg

let update_handle ~did ~handle db =
  match Util.validate_handle handle with
  | Error (InvalidFormat e) ->
      Lwt.return_error (InvalidFormat e)
  | Error (TooShort e) ->
      Lwt.return_error (TooShort e)
  | Error (TooLong e) ->
      Lwt.return_error (TooLong e)
  | Ok () -> (
    match%lwt Data_store.get_actor_by_identifier handle db with
    | Some _ ->
        Lwt.return_error HandleTaken
    | None -> (
        let%lwt {handle= prev_handle; _} =
          Data_store.get_actor_by_identifier did db |> Lwt.map Option.get
        in
        let%lwt () = Data_store.update_actor_handle ~did ~handle db in
        let%lwt plc_result =
          if String.starts_with ~prefix:"did:plc:" did then
            match%lwt Plc.get_audit_log did with
            | Error e ->
                Lwt.return_error
                  (InternalServerError ("failed to fetch did doc: " ^ e))
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
                let aka =
                  List.filter (fun x -> x <> "at://" ^ prev_handle) aka
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
                      (InternalServerError
                         (Printf.sprintf "failed to submit plc operation: %d %s"
                            status msg ) ) )
          else Lwt.return_ok ()
        in
        match plc_result with
        | Error e ->
            Lwt.return_error e
        | Ok () ->
            let () = Ttl_cache.String_cache.remove Id_resolver.Did.cache did in
            let%lwt _ = Sequencer.sequence_identity db ~did ~handle () in
            Lwt.return_ok () ) )

let calc_key_did ctx = Some (Auth.get_authed_did_exn ctx.Xrpc.auth)

let handler =
  Xrpc.handler ~auth:Authorization
    ~rate_limits:
      [ Route
          { duration_ms= 5 * Util.minute
          ; points= 10
          ; calc_key= Some calc_key_did
          ; calc_points= None }
      ; Route
          { duration_ms= Util.day
          ; points= 50
          ; calc_key= Some calc_key_did
          ; calc_points= None } ]
    (fun {req; auth; db; _} ->
      Auth.assert_identity_scope auth ~attr:Oauth.Scopes.Handle ;
      let did = Auth.get_authed_did_exn auth in
      let%lwt {handle} = Xrpc.parse_body req input_of_yojson in
      match%lwt update_handle ~did ~handle db with
      | Ok () ->
          Dream.empty `OK
      | Error e ->
          let msg = update_handle_error_to_string e in
          Log.err (fun log -> log "%s" msg) ;
          Errors.invalid_request ~name:"InvalidHandle" msg )
