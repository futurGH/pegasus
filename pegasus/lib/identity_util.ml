type validate_handle_error =
  | InvalidFormat of string
  | TooShort of string
  | TooLong of string

let validate_handle handle =
  (* if it's a custom domain, just check that it contains a period *)
  if not (String.ends_with ~suffix:("." ^ Env.hostname) handle) then
    if not (String.contains handle '.') then
      Error (InvalidFormat ("must end with " ^ "." ^ Env.hostname))
    else Ok ()
  else
    let front =
      String.sub handle 0
        (String.length handle - (String.length Env.hostname + 1))
    in
    if String.contains front '.' then
      Error (InvalidFormat "can't contain periods")
    else
      match String.length front with
      | l when l < 3 ->
          Error (TooShort "must be at least 3 characters")
      | l when l > 18 ->
          Error (TooLong "must be at most 18 characters")
      | _ ->
          Ok ()

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
  match validate_handle handle with
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
