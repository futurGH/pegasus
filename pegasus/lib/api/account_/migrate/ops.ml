(* local migration operations *)

let get_account_status = Server.CheckAccountStatus.get_account_status

let create_account ~email ~handle ~password ~did ~service_auth_token
    ?invite_code db =
  let open Lwt.Infix in
  let%lwt verified =
    match%lwt
      Jwt.verify_service_jwt ~nsid:"com.atproto.server.createAccount" ~did
        ~verify_sig:(fun _did pk ->
          Lwt.return
          @@ Jwt.verify_jwt service_auth_token
               ~pubkey:(Kleidos.parse_multikey_str pk) )
        service_auth_token
    with
    | Ok creds ->
        Lwt.return_ok creds
    | Error (AuthRequired e) ->
        Lwt.return_error @@ Errors.auth_required e
    | Error (ExpiredToken e) ->
        Lwt.return_error @@ Errors.invalid_request ~name:"ExpiredToken" e
    | Error (InvalidToken e) ->
        Lwt.return_error @@ Errors.invalid_request ~name:"InvalidToken" e
    | Error (InternalError e) ->
        Lwt.return_error @@ Errors.internal_error ~msg:e ()
  in
  match verified with
  | Error e ->
      Lwt.return_error e
  | Ok _ -> (
    match%lwt Data_store.get_actor_by_identifier did db with
    | Some existing when existing.deactivated_at <> None ->
        Lwt.return_error
          "RESUMABLE: An incomplete migration exists for this account."
    | Some _ ->
        Lwt.return_error
          "An account with this DID already exists on this PDS. If you \
           previously migrated here, try logging in instead."
    | None -> (
      match%lwt Data_store.get_actor_by_identifier handle db with
      | Some _ ->
          Lwt.return_error
            ( "The handle @" ^ handle
            ^ " is already taken on this PDS. You may need to use a different \
               handle." )
      | None -> (
        match%lwt Data_store.get_actor_by_identifier email db with
        | Some _ ->
            Lwt.return_error "An account with this email already exists"
        | None -> (
            ( match
                ( Env.invite_required
                , invite_code
                , Option.bind invite_code (fun c ->
                      if String.length c = 0 then None else Some c ) )
              with
              | true, None, _ | true, _, None ->
                  Lwt.return_error "An invite code is required"
              | true, Some code, _ -> (
                match%lwt Data_store.get_invite ~code db with
                | Some i when i.remaining > 0 -> (
                  match%lwt Data_store.use_invite ~code db with
                  | Some _ ->
                      Lwt.return_ok ()
                  | None ->
                      Lwt.return_error "Failed to use invite code" )
                | _ ->
                    Lwt.return_error "Invalid invite code" )
              | false, _, _ ->
                  Lwt.return_ok () )
            >>= function
            | Error e ->
                Lwt.return_error e
            | Ok () ->
                let signing_key, signing_pubkey =
                  Kleidos.K256.generate_keypair ()
                in
                let sk_priv_mk = Kleidos.K256.privkey_to_multikey signing_key in
                let%lwt () =
                  Data_store.create_actor ~did ~handle ~email ~password
                    ~signing_key:sk_priv_mk db
                in
                let%lwt () = Data_store.deactivate_actor did db in
                let () =
                  Util.mkfile_p
                    (Util.Constants.user_db_filepath did)
                    ~perm:0o644
                in
                let%lwt _ = Sequencer.sequence_identity db ~did ~handle () in
                let%lwt _ =
                  Sequencer.sequence_account db ~did ~active:false
                    ~status:`Deactivated ()
                in
                Lwt.return_ok (Kleidos.K256.pubkey_to_did_key signing_pubkey) )
        ) ) )

let friendly_exception = function
  | Failure message | Invalid_argument message ->
      message
  | exn ->
      Printexc.to_string exn

let import_repo ~did ~car_stream =
  let started = Unix.gettimeofday () in
  try%lwt
    let%lwt repo = Repository.load ~create:true did in
    match%lwt Repository.import_car repo car_stream with
    | Ok _ ->
        Log.info (fun log ->
            log "migration %s: repository imported in %.3fs" did
              (Unix.gettimeofday () -. started) ) ;
        Lwt.return_ok ()
    | Error e ->
        Log.err (fun log ->
            log "migration %s: repository import failed after %.3fs: %s" did
              (Unix.gettimeofday () -. started)
              (Printexc.to_string e) ) ;
        Lwt.return_error ("Failed to import repository: " ^ friendly_exception e)
  with exn ->
    Log.err (fun log ->
        log "migration %s: repository import raised after %.3fs: %s" did
          (Unix.gettimeofday () -. started)
          (Printexc.to_string exn) ) ;
    Lwt.return_error ("Failed to import repository: " ^ friendly_exception exn)

let import_blobs_batch ~did ~cids client =
  let%lwt user_db = User_store.connect ~create:true did in
  let cids = List.sort_uniq String.compare cids in
  let pool = Lwt_pool.create 6 (fun () -> Lwt.return_unit) in
  let fetch_one cid_str =
    let rec attempt remaining =
      match%lwt Remote.fetch_blob ~did ~cid:cid_str client with
      | Error _ when remaining > 1 ->
          let%lwt () = Lwt_unix.sleep (0.15 *. float_of_int (4 - remaining)) in
          attempt (remaining - 1)
      | Error e ->
          Log.warn (fun log ->
              log "migration %s: blob %s failed after retries: %s" did cid_str e ) ;
          Lwt.return_error cid_str
      | Ok (data, mimetype) -> (
        match Cid.of_string cid_str with
        | Error e ->
            Log.warn (fun log ->
                log "migration %s: source returned invalid blob CID %s: %s" did
                  cid_str e ) ;
            Lwt.return_error cid_str
        | Ok cid -> (
            let actual = Cid.create cid.codec data in
            if not (Cid.equal actual cid) then (
              Log.warn (fun log ->
                  log "migration %s: blob contents do not match CID %s" did
                    cid_str ) ;
              Lwt.return_error cid_str )
            else
              try%lwt
                let%lwt storage = Blob_store.put ~did ~cid ~data in
                Lwt.return_ok
                  (cid, mimetype, Blob_store.storage_to_string storage)
              with exn ->
                Log.warn (fun log ->
                    log "migration %s: failed to store blob %s: %s" did cid_str
                      (Printexc.to_string exn) ) ;
                Lwt.return_error cid_str ) )
    in
    Lwt_pool.use pool (fun () -> attempt 3)
  in
  let%lwt results = Lwt_list.map_p fetch_one cids in
  let stored = List.filter_map Result.to_option results in
  let imported = List.length stored in
  let failed = List.length cids - imported in
  match%lwt
    Lwt.catch
      (fun () ->
        let%lwt () = User_store.put_blobs user_db stored in
        Lwt.return_ok () )
      (fun exn -> Lwt.return_error exn)
  with
  | Ok () ->
      Log.info (fun log ->
          log "migration %s: blob batch complete imported=%d failed=%d" did
            imported failed ) ;
      Lwt.return (imported, failed)
  | Error exn ->
      Log.err (fun log ->
          log "migration %s: failed to index blob batch: %s" did
            (Printexc.to_string exn) ) ;
      Lwt.return (0, List.length cids)

let list_missing_blobs ~did ~limit ?cursor () =
  try%lwt
    let%lwt {db= us; _} = Repository.load did in
    let cursor = Option.value ~default:"" cursor in
    let%lwt blobs = User_store.list_missing_blobs ~limit ~cursor us in
    let cids = List.map (fun (_, cid) -> Cid.to_string cid) blobs in
    let next_cursor =
      if List.length blobs >= limit then List.nth_opt cids (List.length cids - 1)
      else None
    in
    Lwt.return_ok (cids, next_cursor)
  with exn ->
    Lwt.return_error ("Failed to list missing blobs: " ^ Printexc.to_string exn)

let check_account_status ~did =
  try%lwt
    match%lwt get_account_status did with
    | Ok status ->
        Lwt.return_ok status
    | _ ->
        Lwt.return_error "Failed to load account data"
  with exn ->
    Lwt.return_error
      ("Failed to check account status: " ^ Printexc.to_string exn)

let count_missing_blobs ~did =
  try%lwt
    let%lwt {db= us; _} = Repository.load did in
    let%lwt count = User_store.count_missing_blobs us in
    Lwt.return_ok count
  with exn ->
    Lwt.return_error ("Failed to count missing blobs: " ^ Printexc.to_string exn)

let activate_account did db =
  let%lwt () = Data_store.activate_actor did db in
  let%lwt _ =
    Sequencer.sequence_account db ~did ~active:true ~status:`Active ()
  in
  let%lwt _ = Sequencer.sequence_identity ~did db () in
  Lwt.return_unit

let submit_plc_operation ~did ~handle ~(operation : Plc.signed_operation) db =
  match operation with
  | Tombstone _ ->
      Lwt.return_error "Cannot submit tombstone operation during migration"
  | Operation op -> (
    match Plc.validate_operation ~handle (Operation op) with
    | Ok () -> (
      match%lwt Plc.submit_operation did operation with
      | Ok () ->
          let%lwt _ = Sequencer.sequence_identity db ~did () in
          let%lwt _ = Id_resolver.Did.resolve ~skip_cache:true did in
          Lwt.return_ok ()
      | Error (status, msg) ->
          Lwt.return_error
            (Printf.sprintf "PLC submission failed (%d): %s" status msg) )
    | Error e ->
        Lwt.return_error e )
