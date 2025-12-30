(* account migration handlers *)
open Lexicons

(* helper to serialize session with pds_uri guaranteed to be set *)
let session_to_yojson_with_pds ~old_pds session =
  let session_with_pds =
    match session.Hermes.pds_uri with
    | Some _ ->
        session
    | None ->
        {session with pds_uri= Some old_pds}
  in
  Hermes.session_to_yojson session_with_pds

let make_props ~csrf_token ~invite_required ~hostname
    ?(step = "enter_credentials") ?did ?handle ?old_pds ?identifier ?invite_code
    ?(blobs_imported = 0) ?(blobs_failed = 0) ?(old_account_deactivated = false)
    ?old_account_deactivation_error ?error ?message () :
    Frontend.MigratePage.props =
  { csrf_token
  ; invite_required
  ; hostname
  ; step
  ; did
  ; handle
  ; old_pds
  ; identifier
  ; invite_code
  ; blobs_imported
  ; blobs_failed
  ; old_account_deactivated
  ; old_account_deactivation_error
  ; error
  ; message }

let render_error ~csrf_token ~invite_required ~hostname
    ?(step = "enter_credentials") ?did ?handle ?old_pds ?identifier ?invite_code
    error =
  Util.render_html ~status:`Bad_Request ~title:"Migrate Account"
    (module Frontend.MigratePage)
    ~props:
      (make_props ~csrf_token ~invite_required ~hostname ~step ?did ?handle
         ?old_pds ?identifier ?invite_code ~error () )

(* render_error is passed to step fns with some parameters filled in, this is what remains *)
type render_err =
     ?step:string
  -> ?did:string
  -> ?handle:string
  -> ?old_pds:string
  -> ?identifier:string
  -> ?invite_code:string
  -> string
  -> Dream.response Lwt.t

let resume_from_state ~old_pds session_json =
  match Hermes.session_of_yojson session_json with
  | Error e ->
      Lwt.return_error ("Invalid session state: " ^ e)
  | Ok session ->
      let manager = Hermes.make_credential_manager ~service:old_pds () in
      let%lwt client = Hermes.resume manager ~session () in
      Lwt.return_ok client

(* transition to plc token step after data import *)
let transition_to_plc_token_step ctx ~old_client ~old_pds ~did ~handle ~email
    ~blobs_imported ~blobs_failed =
  let csrf_token = Dream.csrf_token ctx.Xrpc.req in
  let invite_required = Env.invite_required in
  let hostname = Env.hostname in
  let session = Hermes.get_session old_client in
  (* import preferences before transitioning *)
  let%lwt () =
    match%lwt Remote.fetch_preferences old_client with
    | Ok prefs ->
        Data_store.put_preferences ~did
          ~prefs:(App_bsky_actor_defs.preferences_to_yojson prefs)
          ctx.db
    | _ ->
        Lwt.return_unit
  in
  (* don't need plc step for did:web *)
  if String.starts_with ~prefix:"did:web:" did then
    match%lwt State.check_identity_updated did with
    | Ok true ->
        let%lwt () = Ops.activate_account did ctx.db in
        let%lwt () = Session.log_in_did ctx.req did in
        let%lwt deactivation_result = Remote.deactivate_account old_client in
        let old_account_deactivated, old_account_deactivation_error =
          match deactivation_result with
          | Ok () ->
              (true, None)
          | Error e ->
              Dream.warning (fun log ->
                  log "migration %s: failed to deactivate old account: %s" did e ) ;
              (false, Some e)
        in
        Util.render_html ~title:"Migrate Account"
          (module Frontend.MigratePage)
          ~props:
            (make_props ~csrf_token ~invite_required ~hostname ~step:"complete"
               ~did ~handle ~blobs_imported ~blobs_failed
               ~old_account_deactivated ?old_account_deactivation_error
               ~message:
                 "Your account has been successfully migrated! Your did:web \
                  identity is pointing to this PDS."
               () )
    | _ ->
        Util.render_html ~title:"Migrate Account"
          (module Frontend.MigratePage)
          ~props:
            (make_props ~csrf_token ~invite_required ~hostname ~step:"error"
               ~did ~handle ~blobs_imported ~blobs_failed
               ~error:
                 (Printf.sprintf
                    "Your account uses did:web which requires manual \
                     configuration. Please update your .well-known/did.json at \
                     %s to point to this PDS (%s), then try resuming the \
                     migration."
                    (String.sub did 8 (String.length did - 8))
                    Env.host_endpoint )
               () )
  else
    match session with
    | None ->
        Util.render_html ~status:`Internal_Server_Error ~title:"Migrate Account"
          (module Frontend.MigratePage)
          ~props:
            (make_props ~csrf_token ~invite_required ~hostname ~step:"error"
               ~did ~handle ~error:"Internal error: session not found" () )
    | Some session -> (
      match%lwt Remote.request_plc_signature old_client with
      | Error e ->
          Dream.warning (fun log ->
              log "migration %s: failed to request PLC signature: %s" did e ) ;
          let%lwt () =
            State.set ctx.req
              { did
              ; handle
              ; old_pds
              ; session= session_to_yojson_with_pds ~old_pds session
              ; email
              ; blobs_imported
              ; blobs_failed
              ; blobs_cursor= ""
              ; plc_requested= true }
          in
          Util.render_html ~title:"Migrate Account"
            (module Frontend.MigratePage)
            ~props:
              (make_props ~csrf_token ~invite_required ~hostname
                 ~step:"enter_plc_token" ~did ~handle ~old_pds ~blobs_imported
                 ~blobs_failed
                 ~message:
                   "Data import complete! Check your email for a PLC \
                    confirmation code."
                 ~error:
                   ( "Note: Could not automatically request PLC signature: " ^ e
                   ^ ". You may need to request it manually from your old PDS."
                   )
                 () )
      | Ok () ->
          let%lwt () =
            State.set ctx.req
              { did
              ; handle
              ; old_pds
              ; session= session_to_yojson_with_pds ~old_pds session
              ; email
              ; blobs_imported
              ; blobs_failed
              ; blobs_cursor= ""
              ; plc_requested= true }
          in
          Util.render_html ~title:"Migrate Account"
            (module Frontend.MigratePage)
            ~props:
              (make_props ~csrf_token ~invite_required ~hostname
                 ~step:"enter_plc_token" ~did ~handle ~old_pds ~blobs_imported
                 ~blobs_failed
                 ~message:
                   "Data import complete! Check your email for a PLC \
                    confirmation code."
                 () ) )

let rec handle_start_migration ctx ~csrf_token ~invite_required ~hostname
    ~(render_err : render_err) fields =
  let identifier =
    List.assoc_opt "identifier" fields
    |> Option.value ~default:"" |> String.trim
  in
  let password = List.assoc_opt "password" fields |> Option.value ~default:"" in
  let invite_code =
    List.assoc_opt "invite_code" fields
    |> Option.map String.trim
    |> fun c ->
    Option.bind c (fun s -> if String.length s = 0 then None else Some s)
  in
  (* debug steps for testing *)
  let debug_step =
    match invite_code with
    | Some "DEBUG:RESUME" ->
        Some "resume_available"
    | Some "DEBUG:IMPORT" ->
        Some "importing_data"
    | Some "DEBUG:PLC" ->
        Some "enter_plc_token"
    | Some "DEBUG:COMPLETE" ->
        Some "complete"
    | Some "DEBUG:COMPLETE_FAIL" ->
        Some "complete_deactivation_failed"
    | Some "DEBUG:ERROR" ->
        Some "error"
    | _ ->
        None
  in
  match debug_step with
  | Some step ->
      handle_debug_step ctx ~csrf_token ~invite_required ~hostname step
  | None ->
      if String.length identifier = 0 then
        render_err "Please enter your handle or DID"
      else if String.length password = 0 then
        render_err "Please enter your password"
      else
        perform_migration ctx ~csrf_token ~invite_required ~hostname ~render_err
          ~identifier ~password ~invite_code

and handle_debug_step (ctx : Xrpc.context) ~csrf_token ~invite_required
    ~hostname step =
  let mock_session =
    Hermes.session_to_yojson
      { access_jwt= "test_access_jwt"
      ; refresh_jwt= "test_refresh_jwt"
      ; did= "did:plc:a1b2c3"
      ; handle= "test.user"
      ; pds_uri= None
      ; email= None
      ; email_confirmed= None
      ; email_auth_factor= None
      ; active= None
      ; status= None }
  in
  match step with
  | "resume_available" ->
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname
             ~step:"resume_available" ~did:"did:plc:a1b2c3" ~handle:"test.user"
             ~old_pds:"https://pegasus.dev" () )
  | "importing_data" ->
      let%lwt () =
        State.set ctx.req
          { did= "did:plc:a1b2c3"
          ; handle= "test.user"
          ; old_pds= "https://pegasus.dev"
          ; session= mock_session
          ; email= "test@example.com"
          ; blobs_imported= 42
          ; blobs_failed= 3
          ; blobs_cursor= ""
          ; plc_requested= false }
      in
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname
             ~step:"importing_data" ~did:"did:plc:a1b2c3" ~handle:"test.user"
             ~old_pds:"https://pegasus.dev" ~blobs_imported:42 ~blobs_failed:3
             () )
  | "enter_plc_token" ->
      let%lwt () =
        State.set ctx.req
          { did= "did:plc:a1b2c3"
          ; handle= "test.user"
          ; old_pds= "https://pegasus.dev"
          ; session= mock_session
          ; email= "test@example.com"
          ; blobs_imported= 100
          ; blobs_failed= 0
          ; blobs_cursor= ""
          ; plc_requested= true }
      in
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname
             ~step:"enter_plc_token" ~did:"did:plc:testuser123"
             ~handle:"test.user" ~old_pds:"https://pegasus.dev"
             ~blobs_imported:100 ~blobs_failed:0
             ~message:
               "Data import complete! Check your email for a PLC confirmation \
                code."
             () )
  | "complete" ->
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname ~step:"complete"
             ~did:"did:plc:testuser123" ~handle:"test.user" ~blobs_imported:100
             ~blobs_failed:0 ~old_account_deactivated:true
             ~message:"Your account has been successfully migrated!" () )
  | "complete_deactivation_failed" ->
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname ~step:"complete"
             ~did:"did:plc:testuser123" ~handle:"test.user" ~blobs_imported:95
             ~blobs_failed:5 ~old_account_deactivated:false
             ~old_account_deactivation_error:
               "Failed to deactivate old account (401): Unauthorized"
             ~message:"Your account has been successfully migrated!" () )
  | "error" | _ ->
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname ~step:"error" ())

and perform_migration ctx ~csrf_token ~invite_required ~hostname
    ~(render_err : render_err) ~identifier ~password ~invite_code =
  match%lwt Remote.resolve_identity identifier with
  | Error e ->
      render_err e
  | Ok (did, handle, old_pds) -> (
    match%lwt
      Remote.create_session ~service:old_pds ~identifier ~password ()
    with
    | Remote.AuthError e ->
        render_err e
    | Remote.AuthNeeds2FA ->
        Util.render_html ~title:"Migrate Account"
          (module Frontend.MigratePage)
          ~props:
            (make_props ~csrf_token ~invite_required ~hostname ~step:"enter_2fa"
               ~identifier ~old_pds ?invite_code () )
    | Remote.AuthSuccess old_client -> (
      match Hermes.get_session old_client with
      | None ->
          render_err "Internal error: session not found after login"
      | Some session -> (
          let is_active =
            match session.active with Some false -> false | _ -> true
          in
          if not is_active then
            render_err
              "This account is already deactivated. Cannot migrate a \
               deactivated account."
          else
            match%lwt Remote.get_service_auth old_client with
            | Error e ->
                render_err ("Failed to get service authorization: " ^ e)
            | Ok service_auth_token -> (
                let email =
                  match session.email with
                  | Some e when String.length e > 0 ->
                      e
                  | _ ->
                      Printf.sprintf "%s@%s" did Env.hostname
                in
                match%lwt
                  Ops.create_account ~email ~handle ~password ~did
                    ~service_auth_token ?invite_code ctx.db
                with
                | Error e when String.starts_with ~prefix:"RESUMABLE:" e ->
                    handle_resumable_migration ctx ~old_client ~csrf_token
                      ~invite_required ~hostname ~render_err ~did ~handle
                      ~old_pds ~email
                | Error e ->
                    render_err e
                | Ok _signing_key_did ->
                    perform_data_import ctx ~old_client ~csrf_token
                      ~invite_required ~hostname ~render_err ~did ~handle
                      ~old_pds ~email ) ) ) )

and handle_resumable_migration ctx ~old_client ~csrf_token ~invite_required
    ~hostname ~(render_err : render_err) ~did ~handle ~old_pds ~email =
  match%lwt State.check_resume_state ~did ctx.db with
  | Error e ->
      render_err ~did ~handle ~old_pds e
  | Ok State.AlreadyActive ->
      let%lwt () = Session.log_in_did ctx.req did in
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname ~step:"complete"
             ~did ~handle
             ~message:"Your account is already active! You have been logged in."
             () )
  | Ok State.NeedsActivation ->
      let%lwt () = Ops.activate_account did ctx.db in
      let%lwt () = Session.log_in_did ctx.req did in
      let%lwt deactivation_result = Remote.deactivate_account old_client in
      let old_account_deactivated, old_account_deactivation_error =
        match deactivation_result with
        | Ok () ->
            (true, None)
        | Error err ->
            Dream.warning (fun log ->
                log "migration %s: failed to deactivate old account: %s" did err ) ;
            (false, Some err)
      in
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname ~step:"complete"
             ~did ~handle ~old_account_deactivated
             ?old_account_deactivation_error
             ~message:
               "Your account has been activated! Your identity is pointing to \
                this PDS."
             () )
  | Ok State.NeedsPlcUpdate ->
      transition_to_plc_token_step ctx ~old_client ~old_pds ~did ~handle ~email
        ~blobs_imported:0 ~blobs_failed:0
  | Ok State.NeedsRepoImport | Ok State.NeedsBlobImport -> (
    match%lwt Remote.fetch_repo old_client ~did with
    | Error err ->
        render_err ~did ~handle ~old_pds ("Failed to fetch repository: " ^ err)
    | Ok car_data -> (
      match%lwt
        Ops.import_repo ~did ~car_data:(Bytes.of_string (fst car_data))
      with
      | Error err ->
          render_err ~did ~handle ~old_pds err
      | Ok () ->
          transition_to_plc_token_step ctx ~old_client ~old_pds ~did ~handle
            ~email ~blobs_imported:0 ~blobs_failed:0 ) )

and perform_data_import ctx ~old_client ~csrf_token ~invite_required ~hostname
    ~render_err ~did ~handle ~old_pds ~email =
  match%lwt Remote.fetch_repo old_client ~did with
  | Error e ->
      render_err ("Failed to fetch repository: " ^ e)
  | Ok car_data -> (
    match%lwt
      Ops.import_repo ~did ~car_data:(Bytes.of_string (fst car_data))
    with
    | Error e ->
        render_err e
    | Ok () -> (
        let%lwt () =
          match%lwt Ops.check_account_status ~did with
          | Ok status ->
              Dream.info (fun log ->
                  log
                    "migration %s: repo imported, indexed_records=%d, \
                     expected_blobs=%d"
                    did status.indexed_records status.expected_blobs ) ;
              Lwt.return_unit
          | Error e ->
              Dream.warning (fun log ->
                  log "migration %s: failed to check account status: %s" did e ) ;
              Lwt.return_unit
        in
        match%lwt Ops.list_missing_blobs ~did ~limit:50 () with
        | Error e ->
            Dream.warning (fun log ->
                log "migration %s: failed to list missing blobs: %s" did e ) ;
            transition_to_plc_token_step ctx ~old_client ~old_pds ~did ~handle
              ~email ~blobs_imported:0 ~blobs_failed:0
        | Ok (missing_cids, next_cursor) -> (
            if List.length missing_cids = 0 then
              transition_to_plc_token_step ctx ~old_client ~old_pds ~did ~handle
                ~email ~blobs_imported:0 ~blobs_failed:0
            else
              let%lwt imported, failed =
                Ops.import_blobs_batch old_client ~did ~cids:missing_cids
              in
              let cursor = Option.value ~default:"" next_cursor in
              if String.length cursor = 0 then
                transition_to_plc_token_step ctx ~old_client ~old_pds ~did
                  ~handle ~email ~blobs_imported:imported ~blobs_failed:failed
              else
                match Hermes.get_session old_client with
                | None ->
                    render_err "Internal error: session not found"
                | Some session ->
                    let%lwt () =
                      State.set ctx.req
                        { did
                        ; handle
                        ; old_pds
                        ; session= session_to_yojson_with_pds ~old_pds session
                        ; email
                        ; blobs_imported= imported
                        ; blobs_failed= failed
                        ; blobs_cursor= cursor
                        ; plc_requested= false }
                    in
                    Util.render_html ~title:"Migrate Account"
                      (module Frontend.MigratePage)
                      ~props:
                        (make_props ~csrf_token ~invite_required ~hostname
                           ~step:"importing_data" ~did ~handle ~old_pds
                           ~blobs_imported:imported ~blobs_failed:failed () ) )
        ) )

and handle_continue_blobs (ctx : Xrpc.context) ~csrf_token ~invite_required
    ~hostname ~render_err =
  match State.get ctx.req with
  | None ->
      render_err "Migration state not found. Please start over."
  | Some state -> (
      let cursor =
        if String.length state.blobs_cursor > 0 then Some state.blobs_cursor
        else None
      in
      match%lwt resume_from_state ~old_pds:state.old_pds state.session with
      | Error e ->
          render_err e
      | Ok old_client -> (
        match%lwt
          Ops.list_missing_blobs ~did:state.did ~limit:50 ?cursor ()
        with
        | Error e ->
            Dream.warning (fun log ->
                log "migration %s: failed to list missing blobs: %s" state.did e ) ;
            transition_to_plc_token_step ctx ~old_client ~old_pds:state.old_pds
              ~did:state.did ~handle:state.handle ~email:state.email
              ~blobs_imported:state.blobs_imported
              ~blobs_failed:state.blobs_failed
        | Ok (missing_cids, next_cursor) ->
            if List.length missing_cids = 0 then
              transition_to_plc_token_step ctx ~old_client
                ~old_pds:state.old_pds ~did:state.did ~handle:state.handle
                ~email:state.email ~blobs_imported:state.blobs_imported
                ~blobs_failed:state.blobs_failed
            else
              let%lwt imported, failed =
                Ops.import_blobs_batch old_client ~did:state.did
                  ~cids:missing_cids
              in
              let new_imported = state.blobs_imported + imported in
              let new_failed = state.blobs_failed + failed in
              let new_cursor = Option.value ~default:"" next_cursor in
              if String.length new_cursor = 0 then
                transition_to_plc_token_step ctx ~old_client
                  ~old_pds:state.old_pds ~did:state.did ~handle:state.handle
                  ~email:state.email ~blobs_imported:new_imported
                  ~blobs_failed:new_failed
              else
                let%lwt () =
                  State.set ctx.req
                    { state with
                      blobs_imported= new_imported
                    ; blobs_failed= new_failed
                    ; blobs_cursor= new_cursor }
                in
                Util.render_html ~title:"Migrate Account"
                  (module Frontend.MigratePage)
                  ~props:
                    (make_props ~csrf_token ~invite_required ~hostname
                       ~step:"importing_data" ~did:state.did
                       ~handle:state.handle ~old_pds:state.old_pds
                       ~blobs_imported:new_imported ~blobs_failed:new_failed () )
        ) )

and handle_submit_plc_token (ctx : Xrpc.context) ~csrf_token ~invite_required
    ~hostname ~(render_err : render_err) fields =
  match State.get ctx.req with
  | None ->
      render_err "Migration state not found. Please start over."
  | Some state -> (
      let plc_token =
        List.assoc_opt "plc_token" fields
        |> Option.value ~default:"" |> String.trim
      in
      if String.length plc_token = 0 then
        render_err ~step:"enter_plc_token" ~did:state.did ~handle:state.handle
          ~old_pds:state.old_pds "Please enter the PLC token from your email"
      else
        match%lwt resume_from_state ~old_pds:state.old_pds state.session with
        | Error e ->
            render_err e
        | Ok old_client -> (
            let%lwt old_pds_keys =
              match%lwt Remote.get_recommended_credentials old_client with
              | Ok {rotation_keys= Some keys; _} ->
                  Lwt.return keys
              | Ok _ ->
                  Lwt.return []
              | Error e ->
                  Dream.warning (fun log ->
                      log "migration %s: failed to get old PDS credentials: %s"
                        state.did e ) ;
                  Lwt.return []
            in
            let%lwt current_keys =
              match%lwt Remote.get_plc_rotation_keys ~did:state.did with
              | Ok keys ->
                  Lwt.return keys
              | Error _ ->
                  Lwt.return []
            in
            let keys_to_preserve =
              List.filter (fun k -> not (List.mem k old_pds_keys)) current_keys
            in
            match%lwt Data_store.get_actor_by_identifier state.did ctx.db with
            | None ->
                render_err ~step:"enter_plc_token" ~did:state.did
                  ~handle:state.handle ~old_pds:state.old_pds
                  "Failed to get credentials: account not found"
            | Some actor -> (
                let credentials =
                  Plc.get_recommended_credentials ~handle:actor.handle
                    ~signing_key:actor.signing_key
                    ~extra_rotation_keys:keys_to_preserve ()
                in
                match%lwt
                  Remote.sign_plc_operation old_client ~token:plc_token
                    ~credentials
                with
                | Error e ->
                    render_err ~step:"enter_plc_token" ~did:state.did
                      ~handle:state.handle ~old_pds:state.old_pds
                      ("Failed to sign PLC operation: " ^ e)
                | Ok signed_operation -> (
                  match%lwt
                    Plc.signed_operation_of_yojson signed_operation
                    |> Lwt_result.lift
                    |> fun r ->
                    Lwt_result.bind r (fun op ->
                        Ops.submit_plc_operation ~did:state.did
                          ~handle:state.handle ~operation:op ctx.db )
                  with
                  | Error e ->
                      render_err ~step:"enter_plc_token" ~did:state.did
                        ~handle:state.handle ~old_pds:state.old_pds
                        ("Failed to submit PLC operation: " ^ e)
                  | Ok () ->
                      let%lwt () =
                        match%lwt Ops.check_account_status ~did:state.did with
                        | Ok status ->
                            Dream.info (fun log ->
                                log
                                  "migration %s: activating account, \
                                   imported_blobs=%d/%d"
                                  state.did status.imported_blobs
                                  status.expected_blobs ) ;
                            Lwt.return_unit
                        | Error e ->
                            Dream.warning (fun log ->
                                log
                                  "migration %s: failed to check status before \
                                   activation: %s"
                                  state.did e ) ;
                            Lwt.return_unit
                      in
                      let%lwt () = Ops.activate_account state.did ctx.db in
                      let%lwt () = Session.log_in_did ctx.req state.did in
                      let%lwt () = State.clear ctx.req in
                      let%lwt deactivation_result =
                        Remote.deactivate_account old_client
                      in
                      let ( old_account_deactivated
                          , old_account_deactivation_error ) =
                        match deactivation_result with
                        | Ok () ->
                            (true, None)
                        | Error e ->
                            Dream.warning (fun log ->
                                log
                                  "migration %s: failed to deactivate old \
                                   account: %s"
                                  state.did e ) ;
                            (false, Some e)
                      in
                      Util.render_html ~title:"Migrate Account"
                        (module Frontend.MigratePage)
                        ~props:
                          (make_props ~csrf_token ~invite_required ~hostname
                             ~step:"complete" ~did:state.did
                             ~handle:state.handle
                             ~blobs_imported:state.blobs_imported
                             ~blobs_failed:state.blobs_failed
                             ~old_account_deactivated
                             ?old_account_deactivation_error
                             ~message:
                               "Your account has been successfully migrated!"
                             () ) ) ) ) )

and handle_resend_plc_token (ctx : Xrpc.context) ~csrf_token ~invite_required
    ~hostname =
  match State.get ctx.req with
  | None ->
      render_error ~csrf_token ~invite_required ~hostname
        "Migration state not found. Please start over."
  | Some state -> (
    match%lwt resume_from_state ~old_pds:state.old_pds state.session with
    | Error e ->
        render_error ~csrf_token ~invite_required ~hostname e
    | Ok old_client -> (
      match%lwt Remote.request_plc_signature old_client with
      | Error e ->
          Util.render_html ~title:"Migrate Account"
            (module Frontend.MigratePage)
            ~props:
              (make_props ~csrf_token ~invite_required ~hostname
                 ~step:"enter_plc_token" ~did:state.did ~handle:state.handle
                 ~old_pds:state.old_pds ~error:("Failed to resend: " ^ e) () )
      | Ok () ->
          Util.render_html ~title:"Migrate Account"
            (module Frontend.MigratePage)
            ~props:
              (make_props ~csrf_token ~invite_required ~hostname
                 ~step:"enter_plc_token" ~did:state.did ~handle:state.handle
                 ~old_pds:state.old_pds
                 ~message:"Confirmation code resent! Check your email." () ) ) )

and handle_submit_2fa (ctx : Xrpc.context) ~(render_err : render_err)
    ~csrf_token ~invite_required ~hostname fields =
  let identifier =
    List.assoc_opt "identifier" fields
    |> Option.value ~default:"" |> String.trim
  in
  let old_pds =
    List.assoc_opt "old_pds" fields |> Option.value ~default:"" |> String.trim
  in
  let auth_factor_token =
    List.assoc_opt "auth_factor_token" fields
    |> Option.value ~default:"" |> String.trim
  in
  let invite_code =
    List.assoc_opt "invite_code" fields |> Option.map String.trim
  in
  let password = List.assoc_opt "password" fields |> Option.value ~default:"" in
  if String.length auth_factor_token = 0 then
    render_err ~step:"enter_2fa" ~identifier ~old_pds ?invite_code
      "Please enter your authentication code"
  else
    match%lwt Remote.resolve_identity identifier with
    | Error e ->
        render_err ~step:"enter_2fa" ~identifier ~old_pds ?invite_code e
    | Ok (did, handle, resolved_pds) -> (
        let pds_endpoint =
          if String.length old_pds > 0 then old_pds else resolved_pds
        in
        match%lwt
          Remote.create_session ~service:pds_endpoint ~identifier ~password
            ~auth_factor_token ()
        with
        | Remote.AuthError e ->
            render_err ~step:"enter_2fa" ~identifier ~old_pds:pds_endpoint
              ?invite_code e
        | Remote.AuthNeeds2FA ->
            render_err ~step:"enter_2fa" ~identifier ~old_pds:pds_endpoint
              ?invite_code "Invalid authentication code. Please try again."
        | Remote.AuthSuccess client -> (
          match Hermes.get_session client with
          | None ->
              render_err "Internal error: session not found after login"
          | Some session -> (
              let is_active =
                match session.active with Some false -> false | _ -> true
              in
              if not is_active then
                render_err
                  "This account is already deactivated. Cannot migrate a \
                   deactivated account."
              else
                match%lwt Remote.get_service_auth client with
                | Error e ->
                    render_err ("Failed to get service authorization: " ^ e)
                | Ok service_auth_token -> (
                    let email =
                      match session.email with
                      | Some e when String.length e > 0 ->
                          e
                      | _ ->
                          Printf.sprintf "%s@%s" did Env.hostname
                    in
                    match%lwt
                      Ops.create_account ~email ~handle ~password ~did
                        ~service_auth_token ?invite_code ctx.db
                    with
                    | Error e when String.starts_with ~prefix:"RESUMABLE:" e ->
                        handle_resumable_migration ctx ~old_client:client
                          ~csrf_token ~invite_required ~hostname ~render_err
                          ~did ~handle ~old_pds ~email
                    | Error e ->
                        render_err e
                    | Ok _signing_key_did -> (
                      match%lwt Remote.fetch_repo client ~did with
                      | Error e ->
                          render_err ("Failed to fetch repository: " ^ e)
                      | Ok car_data -> (
                        match%lwt
                          Ops.import_repo ~did
                            ~car_data:(Bytes.of_string @@ fst car_data)
                        with
                        | Error e ->
                            render_err e
                        | Ok () ->
                            transition_to_plc_token_step ctx ~old_client:client
                              ~old_pds:pds_endpoint ~did ~handle ~email
                              ~blobs_imported:0 ~blobs_failed:0 ) ) ) ) ) )

and handle_resume_migration (ctx : Xrpc.context) ~csrf_token ~invite_required
    ~hostname ~(render_err : render_err) fields =
  let identifier =
    List.assoc_opt "identifier" fields
    |> Option.value ~default:"" |> String.trim
  in
  let password = List.assoc_opt "password" fields |> Option.value ~default:"" in
  if String.length identifier = 0 then
    render_err ~step:"resume_available" "Please enter your handle or DID"
  else if String.length password = 0 then
    render_err ~step:"resume_available" "Please enter your password"
  else
    match%lwt Remote.resolve_identity identifier with
    | Error e ->
        render_err ~step:"resume_available" e
    | Ok (did, handle, old_pds) -> (
      match%lwt
        Remote.create_session ~service:old_pds ~identifier ~password ()
      with
      | Remote.AuthError e ->
          render_err ~step:"resume_available" e
      | Remote.AuthNeeds2FA ->
          Util.render_html ~title:"Migrate Account"
            (module Frontend.MigratePage)
            ~props:
              (make_props ~csrf_token ~invite_required ~hostname
                 ~step:"enter_2fa" ~identifier ~old_pds () )
      | Remote.AuthSuccess client -> (
        match Hermes.get_session client with
        | None ->
            render_err ~step:"resume_available"
              "Internal error: session not found after login"
        | Some session -> (
            let email =
              match session.email with
              | Some e when String.length e > 0 ->
                  e
              | _ ->
                  Printf.sprintf "%s@%s" did Env.hostname
            in
            match%lwt State.check_resume_state ~did ctx.db with
            | Error e ->
                render_err ~step:"resume_available" ~did ~handle ~old_pds e
            | Ok State.AlreadyActive ->
                let%lwt () = Session.log_in_did ctx.req did in
                Util.render_html ~title:"Migrate Account"
                  (module Frontend.MigratePage)
                  ~props:
                    (make_props ~csrf_token ~invite_required ~hostname
                       ~step:"complete" ~did ~handle
                       ~message:
                         "Your account is already active! You have been logged \
                          in."
                       () )
            | Ok State.NeedsActivation ->
                let%lwt () = Ops.activate_account did ctx.db in
                let%lwt () = Session.log_in_did ctx.req did in
                let%lwt deactivation_result =
                  Remote.deactivate_account client
                in
                let old_account_deactivated, old_account_deactivation_error =
                  match deactivation_result with
                  | Ok () ->
                      (true, None)
                  | Error e ->
                      Dream.warning (fun log ->
                          log
                            "migration %s: failed to deactivate old account: %s"
                            did e ) ;
                      (false, Some e)
                in
                Util.render_html ~title:"Migrate Account"
                  (module Frontend.MigratePage)
                  ~props:
                    (make_props ~csrf_token ~invite_required ~hostname
                       ~step:"complete" ~did ~handle ~old_account_deactivated
                       ?old_account_deactivation_error
                       ~message:
                         "Your account has been activated! Your identity is \
                          pointing to this PDS."
                       () )
            | Ok State.NeedsPlcUpdate ->
                transition_to_plc_token_step ctx ~old_client:client ~old_pds
                  ~did ~handle ~email ~blobs_imported:0 ~blobs_failed:0
            | Ok State.NeedsRepoImport | Ok State.NeedsBlobImport -> (
              match%lwt Remote.fetch_repo client ~did with
              | Error e ->
                  render_err ~did ~handle ~old_pds
                    ("Failed to fetch repository: " ^ e)
              | Ok car_data -> (
                match%lwt
                  Ops.import_repo ~did
                    ~car_data:(Bytes.of_string @@ fst car_data)
                with
                | Error e ->
                    render_err ~did ~handle ~old_pds e
                | Ok () -> (
                  match%lwt Ops.list_missing_blobs ~did ~limit:50 () with
                  | Error e ->
                      Dream.warning (fun log ->
                          log "migration %s: failed to list missing blobs: %s"
                            did e ) ;
                      transition_to_plc_token_step ctx ~old_client:client
                        ~old_pds ~did ~handle ~email ~blobs_imported:0
                        ~blobs_failed:0
                  | Ok (missing_cids, next_cursor) ->
                      if List.length missing_cids = 0 then
                        transition_to_plc_token_step ctx ~old_client:client
                          ~old_pds ~did ~handle ~email ~blobs_imported:0
                          ~blobs_failed:0
                      else
                        let%lwt imported, failed =
                          Ops.import_blobs_batch client ~did ~cids:missing_cids
                        in
                        let cursor = Option.value ~default:"" next_cursor in
                        if String.length cursor = 0 then
                          transition_to_plc_token_step ctx ~old_client:client
                            ~old_pds ~did ~handle ~email
                            ~blobs_imported:imported ~blobs_failed:failed
                        else
                          let%lwt () =
                            State.set ctx.req
                              { did
                              ; handle
                              ; old_pds
                              ; session=
                                  session_to_yojson_with_pds ~old_pds session
                              ; email
                              ; blobs_imported= imported
                              ; blobs_failed= failed
                              ; blobs_cursor= cursor
                              ; plc_requested= false }
                          in
                          Util.render_html ~title:"Migrate Account"
                            (module Frontend.MigratePage)
                            ~props:
                              (make_props ~csrf_token ~invite_required ~hostname
                                 ~step:"importing_data" ~did ~handle ~old_pds
                                 ~blobs_imported:imported ~blobs_failed:failed
                                 () ) ) ) ) ) ) )

let get_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let invite_required = Env.invite_required in
      let hostname = Env.hostname in
      let props : Frontend.MigratePage.props =
        match State.get ctx.req with
        | None ->
            make_props ~csrf_token ~invite_required ~hostname
              ~step:"enter_credentials" ()
        | Some state ->
            if state.plc_requested then
              make_props ~csrf_token ~invite_required ~hostname
                ~step:"enter_plc_token" ~did:state.did ~handle:state.handle
                ~old_pds:state.old_pds ~blobs_imported:state.blobs_imported
                ~blobs_failed:state.blobs_failed ()
            else
              make_props ~csrf_token ~invite_required ~hostname
                ~step:"importing_data" ~did:state.did ~handle:state.handle
                ~old_pds:state.old_pds ~blobs_imported:state.blobs_imported
                ~blobs_failed:state.blobs_failed ()
      in
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props )

let post_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let invite_required = Env.invite_required in
      let hostname = Env.hostname in
      let render_err = render_error ~csrf_token ~invite_required ~hostname in
      match%lwt Dream.form ctx.req with
      | `Ok fields -> (
          let get_field name =
            List.assoc_opt name fields |> Option.value ~default:""
            |> String.trim
          in
          let action = get_field "action" in
          match action with
          | "start_migration" ->
              handle_start_migration ctx ~csrf_token ~invite_required ~hostname
                ~render_err fields
          | "continue_blobs" ->
              handle_continue_blobs ctx ~csrf_token ~invite_required ~hostname
                ~render_err
          | "submit_plc_token" ->
              handle_submit_plc_token ctx ~csrf_token ~invite_required ~hostname
                ~render_err fields
          | "resend_plc_token" ->
              handle_resend_plc_token ctx ~csrf_token ~invite_required ~hostname
          | "submit_2fa" ->
              handle_submit_2fa ctx ~render_err ~csrf_token ~invite_required
                ~hostname fields
          | "resume_migration" ->
              handle_resume_migration ctx ~csrf_token ~invite_required ~hostname
                ~render_err fields
          | _ ->
              render_err "Invalid action" )
      | _ ->
          render_err "Invalid form submission" )
