open Lexicons.Com.Atproto.Server.CreateAccount.Main

type create_account_error =
  | InviteCodeRequired
  | InvalidInviteCode
  | InvalidHandle of string
  | EmailAlreadyExists
  | HandleAlreadyExists
  | DidAlreadyExists
  | PlcError of string
  | InviteUseFailure

type create_account_result = {did: string; handle: string}

let create_account ~email ~handle ~password ?invite_code ?did ?recovery_key db =
  (* validate invite code if required *)
  let%lwt invite_valid =
    match invite_code with
    | None when Env.invite_required = true ->
        Lwt.return_error InviteCodeRequired
    | Some code when Env.invite_required = true -> (
        let%lwt invite = Data_store.get_invite ~code db in
        match invite with
        | Some i when i.remaining > 0 ->
            Lwt.return_ok ()
        | _ ->
            Lwt.return_error InvalidInviteCode )
    | _ ->
        Lwt.return_ok ()
  in
  match invite_valid with
  | Error e ->
      Lwt.return_error e
  | Ok () -> (
    (* validate handle *)
    match Identity_util.validate_handle handle with
    | Error (InvalidFormat e) | Error (TooLong e) | Error (TooShort e) ->
        Lwt.return_error (InvalidHandle ("handle " ^ e))
    | Ok _ -> (
      (* check for existing accounts *)
      match%lwt
        Lwt.all
          [ Data_store.get_actor_by_identifier email db
          ; Data_store.get_actor_by_identifier handle db ]
      with
      | [Some _; _] ->
          Lwt.return_error EmailAlreadyExists
      | [_; Some _] ->
          Lwt.return_error HandleAlreadyExists
      | _ -> (
          let signing_key, signing_pubkey = Kleidos.K256.generate_keypair () in
          (* create or validate DID *)
          let%lwt did_result =
            match did with
            | Some did -> (
              match%lwt Data_store.get_actor_by_identifier did db with
              | Some _ ->
                  Lwt.return_error DidAlreadyExists
              | None ->
                  Lwt.return_ok did )
            | None -> (
                let sk_did = Kleidos.K256.pubkey_to_did_key signing_pubkey in
                let rotation_did_keys =
                  match recovery_key with Some rk -> [rk] | None -> []
                in
                match%lwt
                  Plc.submit_genesis Env.rotation_key sk_did ~rotation_did_keys
                    handle
                with
                | Ok did ->
                    Lwt.return_ok did
                | Error e ->
                    Lwt.return_error (PlcError e) )
          in
          match did_result with
          | Error e ->
              Lwt.return_error e
          | Ok did -> (
              (* use invite code *)
              let%lwt invite_used =
                match invite_code with
                | Some code -> (
                  match%lwt Data_store.use_invite ~code db with
                  | Some _ ->
                      Lwt.return_ok ()
                  | None ->
                      Lwt.return_error InviteUseFailure )
                | None ->
                    Lwt.return_ok ()
              in
              match invite_used with
              | Error e ->
                  Lwt.return_error e
              | Ok () ->
                  (* create actor *)
                  let sk_priv_mk =
                    Kleidos.K256.privkey_to_multikey signing_key
                  in
                  let%lwt () =
                    Data_store.create_actor ~did ~handle ~email ~password
                      ~signing_key:sk_priv_mk db
                  in
                  let () =
                    Util.mkfile_p
                      (Util.Constants.user_db_filepath did)
                      ~perm:0o644
                  in
                  let%lwt repo = Repository.load ~create:true did in
                  let%lwt _ = Repository.put_initial_commit repo in
                  let%lwt _ = Sequencer.sequence_identity db ~did ~handle () in
                  let%lwt _ =
                    Sequencer.sequence_account db ~did ~active:true ()
                  in
                  let%lwt {commit= commit_cid, commit; _} =
                    Repository.apply_writes repo [] None
                  in
                  let commit_block =
                    commit |> User_store.Types.signed_commit_to_yojson
                    |> Dag_cbor.encode_yojson
                  in
                  let block_stream =
                    Lwt_seq.of_list [(commit_cid, commit_block)]
                  in
                  let%lwt blocks =
                    Car.blocks_to_stream commit_cid block_stream
                    |> Car.collect_stream
                  in
                  let%lwt _ =
                    Sequencer.sequence_sync db ~did ~rev:commit.rev ~blocks ()
                  in
                  Lwt.return_ok {did; handle} ) ) ) )

let handler =
  Xrpc.handler (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req input_of_yojson in
      match%lwt
        create_account ~email:(Option.get input.email) ~handle:input.handle
          ~password:(Option.get input.password)
          ?invite_code:input.invite_code ?did:input.did
          ?recovery_key:input.recovery_key ctx.db
      with
      | Error InviteCodeRequired ->
          Errors.invalid_request ~name:"InvalidInviteCode"
            "no invite code provided"
      | Error InvalidInviteCode ->
          Errors.invalid_request ~name:"InvalidInviteCode" "invalid invite code"
      | Error (InvalidHandle e) ->
          Errors.invalid_request ~name:"InvalidHandle" e
      | Error EmailAlreadyExists ->
          Errors.invalid_request "an account with that email already exists"
      | Error HandleAlreadyExists ->
          Errors.invalid_request "an account with that handle already exists"
      | Error DidAlreadyExists ->
          Errors.invalid_request "an account with that did already exists"
      | Error (PlcError e) ->
          failwith e
      | Error InviteUseFailure ->
          failwith "failed to use invite code"
      | Ok {did; handle} ->
          let access_jwt, refresh_jwt = Jwt.generate_jwt did in
          Dream.json @@ Yojson.Safe.to_string
          @@ output_to_yojson
               {access_jwt; refresh_jwt; did; handle; did_doc= None} )
