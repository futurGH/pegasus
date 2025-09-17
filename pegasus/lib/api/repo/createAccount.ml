type request =
  { email: string
  ; handle: string
  ; did: string option
  ; password: string
  ; invite_code: string option [@key "inviteCode"]
  ; recovery_key: string option [@key "recoveryKey"] }
[@@deriving yojson {strict= false}]

type response =
  { access_jwt: string [@key "accessJwt"]
  ; refresh_jwt: string [@key "refreshJwt"]
  ; handle: string
  ; did: string }
[@@deriving yojson]

let handler =
  Xrpc.handler (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req request_of_yojson in
      if input.invite_code = None && Env.invite_required = true then
        Errors.invalid_request ~name:"InvalidInviteCode"
          "no invite code provided" ;
      let () =
        match Util.validate_handle input.handle with
        | Ok _ ->
            ()
        | Error e ->
            raise e
      in
      let%lwt () =
        match%lwt
          Lwt.all
            [ Data_store.get_actor_by_identifier input.email ctx.db
            ; Data_store.get_actor_by_identifier input.handle ctx.db ]
        with
        | [Some _; _] ->
            Errors.invalid_request "an account with that email already exists"
        | [_; Some _] ->
            Errors.invalid_request "an account with that handle already exists"
        | _ ->
            Lwt.return ()
      in
      let signing_key, signing_pubkey = Kleidos.P256.generate_keypair () in
      let%lwt did =
        match input.did with
        | Some did -> (
            match%lwt Data_store.get_actor_by_identifier did ctx.db with
            | Some _ ->
                Errors.invalid_request "an account with that did already exists"
            | None ->
                Lwt.return did )
        | None -> (
            let sk_did = Kleidos.P256.pubkey_to_did_key signing_pubkey in
            let rotation_did_keys =
              [Kleidos.pubkey_to_did_key Env.rotation_key]
            in
            let rotation_did_keys =
              match input.recovery_key with
              | Some rk ->
                  rk :: rotation_did_keys
              | None ->
                  rotation_did_keys
            in
            match%lwt
              Plc.submit_genesis Env.rotation_key sk_did ~rotation_did_keys
                input.handle
            with
            | Ok did ->
                Lwt.return did
            | Error e ->
                failwith e )
      in
      let sk_priv_mk = Kleidos.P256.privkey_to_multikey signing_key in
      let%lwt () =
        Data_store.create_actor ~did ~handle:input.handle ~email:input.email
          ~password:input.password ~signing_key:sk_priv_mk ctx.db
      in
      let%lwt _ = User_store.connect ~create:true ~write:true did in
      let%lwt repo = Repository.load ~write:true ~ds:ctx.db did in
      let%lwt _ = Repository.put_initial_commit repo in
      let%lwt _ =
        Sequencer.sequence_identity ctx.db ~did ~handle:input.handle ()
      in
      let%lwt _ = Sequencer.sequence_account ctx.db ~did ~active:true () in
      let%lwt {commit= commit_cid, commit; _} =
        Repository.apply_writes repo [] None
      in
      let open User_store.Types in
      let commit_block =
        commit |> User_store.Types.signed_commit_to_yojson
        |> Dag_cbor.encode_yojson
      in
      let block_stream = Lwt_seq.of_list [(commit_cid, commit_block)] in
      let%lwt blocks =
        Car.blocks_to_stream commit_cid block_stream |> Car.collect_stream
      in
      let%lwt _ =
        Sequencer.sequence_sync ctx.db ~did ~rev:commit.rev ~blocks ()
      in
      let access_jwt, refresh_jwt = Auth.generate_jwt did in
      Dream.json @@ Yojson.Safe.to_string
      @@ response_to_yojson {access_jwt; refresh_jwt; did; handle= input.handle} )
