(* account migration handlers *)

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

(* transition to plc token step after data import *)
let transition_to_plc_token_step ctx ~did ~handle ~old_pds ~access_jwt
    ~refresh_jwt ~email ~blobs_imported ~blobs_failed =
  let csrf_token = Dream.csrf_token ctx.Xrpc.req in
  let invite_required = Env.invite_required in
  let hostname = Env.hostname in
  (* import preferences before transitioning *)
  let%lwt () =
    match%lwt Remote.fetch_preferences ~pds_endpoint:old_pds ~access_jwt with
    | Ok prefs ->
        Data_store.put_preferences ~did ~prefs ctx.db
    | _ ->
        Lwt.return_unit
  in
  (* don't need plc step for did:web *)
  if String.starts_with ~prefix:"did:web:" did then
    match%lwt State.check_identity_updated did with
    | Ok true ->
        let%lwt () = Ops.activate_account did ctx.db in
        let%lwt () = Session.log_in_did ctx.req did in
        let%lwt deactivation_result =
          Remote.deactivate_account ~pds_endpoint:old_pds ~access_jwt
        in
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
    match%lwt
      Remote.request_plc_signature ~pds_endpoint:old_pds ~access_jwt
    with
    | Error e ->
        Dream.warning (fun log ->
            log "migration %s: failed to request PLC signature: %s" did e ) ;
        let%lwt () =
          State.set ctx.req
            { did
            ; handle
            ; old_pds
            ; access_jwt
            ; refresh_jwt
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
                 ^ ". You may need to request it manually from your old PDS." )
               () )
    | Ok () ->
        let%lwt () =
          State.set ctx.req
            { did
            ; handle
            ; old_pds
            ; access_jwt
            ; refresh_jwt
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
               () )

(* try to deactivate old account, refreshing token if needed *)
let deactivate_old_account_with_refresh ~old_pds ~access_jwt ~refresh_jwt =
  match%lwt Remote.deactivate_account ~pds_endpoint:old_pds ~access_jwt with
  | Ok () ->
      Lwt.return_ok ()
  | Error e
    when Util.str_contains ~affix:"401" e
         || Util.str_contains ~affix:"Unauthorized" e -> (
    match%lwt Remote.refresh_session ~pds_endpoint:old_pds ~refresh_jwt with
    | Ok tokens ->
        Remote.deactivate_account ~pds_endpoint:old_pds
          ~access_jwt:tokens.access_jwt
    | Error refresh_err ->
        Lwt.return_error
          (Printf.sprintf "Token expired and refresh failed: %s" refresh_err) )
  | Error e ->
      Lwt.return_error e

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
  match step with
  | "resume_available" ->
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props:
          (make_props ~csrf_token ~invite_required ~hostname
             ~step:"resume_available" ~did:"did:plc:a1b2c3" ~handle:"test.user"
             ~old_pds:"https://bsky.social" () )
  | "importing_data" ->
      let%lwt () =
        State.set ctx.req
          { did= "did:plc:a1b2c3"
          ; handle= "test.user"
          ; old_pds= "https://bsky.social"
          ; access_jwt= "test_access_jwt"
          ; refresh_jwt= "test_refresh_jwt"
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
             ~old_pds:"https://bsky.social" ~blobs_imported:42 ~blobs_failed:3
             () )
  | "enter_plc_token" ->
      let%lwt () =
        State.set ctx.req
          { did= "did:plc:a1b2c3"
          ; handle= "test.user"
          ; old_pds= "https://bsky.social"
          ; access_jwt= "test_access_jwt"
          ; refresh_jwt= "test_refresh_jwt"
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
             ~handle:"test.user" ~old_pds:"https://bsky.social"
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
      Remote.create_session ~pds_endpoint:old_pds ~identifier ~password ()
    with
    | Remote.AuthError e ->
        render_err e
    | Remote.AuthNeeds2FA ->
        Util.render_html ~title:"Migrate Account"
          (module Frontend.MigratePage)
          ~props:
            (make_props ~csrf_token ~invite_required ~hostname ~step:"enter_2fa"
               ~identifier ~old_pds ?invite_code () )
    | Remote.AuthSuccess session -> (
      match%lwt
        Remote.get_session ~pds_endpoint:old_pds ~access_jwt:session.access_jwt
      with
      | Error e ->
          render_err ("Failed to get account info: " ^ e)
      | Ok session_info -> (
          let is_active =
            match session_info.active with Some false -> false | _ -> true
          in
          if not is_active then
            render_err
              "This account is already deactivated. Cannot migrate a \
               deactivated account."
          else
            match%lwt
              Remote.get_service_auth ~pds_endpoint:old_pds
                ~access_jwt:session.access_jwt
            with
            | Error e ->
                render_err ("Failed to get service authorization: " ^ e)
            | Ok service_auth_token -> (
                let email =
                  match session_info.email with
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
                    handle_resumable_migration ctx ~csrf_token ~invite_required
                      ~hostname ~render_err ~did ~handle ~old_pds ~session
                      ~email
                | Error e ->
                    render_err e
                | Ok _signing_key_did ->
                    perform_data_import ctx ~csrf_token ~invite_required
                      ~hostname ~render_err ~did ~handle ~old_pds ~session
                      ~email ) ) ) )

and handle_resumable_migration ctx ~csrf_token ~invite_required ~hostname
    ~(render_err : render_err) ~did ~handle ~old_pds ~session ~email =
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
      let%lwt deactivation_result =
        deactivate_old_account_with_refresh ~old_pds
          ~access_jwt:session.access_jwt ~refresh_jwt:session.refresh_jwt
      in
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
      transition_to_plc_token_step ctx ~did ~handle ~old_pds
        ~access_jwt:session.access_jwt ~refresh_jwt:session.refresh_jwt ~email
        ~blobs_imported:0 ~blobs_failed:0
  | Ok State.NeedsRepoImport | Ok State.NeedsBlobImport -> (
    match%lwt
      Remote.fetch_repo ~pds_endpoint:old_pds ~access_jwt:session.access_jwt
        ~did
    with
    | Error err ->
        render_err ~did ~handle ~old_pds ("Failed to fetch repository: " ^ err)
    | Ok car_data -> (
      match%lwt Ops.import_repo ~did ~car_data with
      | Error err ->
          render_err ~did ~handle ~old_pds err
      | Ok () ->
          transition_to_plc_token_step ctx ~did ~handle ~old_pds
            ~access_jwt:session.access_jwt ~refresh_jwt:session.refresh_jwt
            ~email ~blobs_imported:0 ~blobs_failed:0 ) )

and perform_data_import ctx ~csrf_token ~invite_required ~hostname ~render_err
    ~did ~handle ~old_pds ~session ~email =
  match%lwt
    Remote.fetch_repo ~pds_endpoint:old_pds ~access_jwt:session.access_jwt ~did
  with
  | Error e ->
      render_err ("Failed to fetch repository: " ^ e)
  | Ok car_data -> (
    match%lwt Ops.import_repo ~did ~car_data with
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
            transition_to_plc_token_step ctx ~did ~handle ~old_pds
              ~access_jwt:session.access_jwt ~refresh_jwt:session.refresh_jwt
              ~email ~blobs_imported:0 ~blobs_failed:0
        | Ok (missing_cids, next_cursor) ->
            if List.length missing_cids = 0 then
              transition_to_plc_token_step ctx ~did ~handle ~old_pds
                ~access_jwt:session.access_jwt ~refresh_jwt:session.refresh_jwt
                ~email ~blobs_imported:0 ~blobs_failed:0
            else
              let%lwt imported, failed =
                Ops.import_blobs_batch ~pds_endpoint:old_pds
                  ~access_jwt:session.access_jwt ~did ~cids:missing_cids
              in
              let cursor = Option.value ~default:"" next_cursor in
              if String.length cursor = 0 then
                transition_to_plc_token_step ctx ~did ~handle ~old_pds
                  ~access_jwt:session.access_jwt
                  ~refresh_jwt:session.refresh_jwt ~email
                  ~blobs_imported:imported ~blobs_failed:failed
              else
                let%lwt () =
                  State.set ctx.req
                    { did
                    ; handle
                    ; old_pds
                    ; access_jwt= session.access_jwt
                    ; refresh_jwt= session.refresh_jwt
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
                       ~blobs_imported:imported ~blobs_failed:failed () ) ) )

and handle_continue_blobs (ctx : Xrpc.context) ~csrf_token ~invite_required
    ~hostname ~render_err =
  match State.get ctx.req with
  | None ->
      render_err "Migration state not found. Please start over."
  | Some state -> (
      let%lwt state =
        if Remote.jwt_needs_refresh state.access_jwt then (
          match%lwt
            Remote.refresh_session ~pds_endpoint:state.old_pds
              ~refresh_jwt:state.refresh_jwt
          with
          | Ok tokens ->
              let new_state =
                { state with
                  access_jwt= tokens.access_jwt
                ; refresh_jwt= tokens.refresh_jwt }
              in
              let%lwt () = State.set ctx.req new_state in
              Lwt.return new_state
          | Error e ->
              Dream.warning (fun log ->
                  log
                    "migration %s: token refresh failed, continuing with old \
                     token: %s"
                    state.did e ) ;
              Lwt.return state )
        else Lwt.return state
      in
      let cursor =
        if String.length state.blobs_cursor > 0 then Some state.blobs_cursor
        else None
      in
      match%lwt Ops.list_missing_blobs ~did:state.did ~limit:50 ?cursor () with
      | Error e ->
          Dream.warning (fun log ->
              log "migration %s: failed to list missing blobs: %s" state.did e ) ;
          transition_to_plc_token_step ctx ~did:state.did ~handle:state.handle
            ~old_pds:state.old_pds ~access_jwt:state.access_jwt
            ~refresh_jwt:state.refresh_jwt ~email:state.email
            ~blobs_imported:state.blobs_imported
            ~blobs_failed:state.blobs_failed
      | Ok (missing_cids, next_cursor) ->
          if List.length missing_cids = 0 then
            transition_to_plc_token_step ctx ~did:state.did ~handle:state.handle
              ~old_pds:state.old_pds ~access_jwt:state.access_jwt
              ~refresh_jwt:state.refresh_jwt ~email:state.email
              ~blobs_imported:state.blobs_imported
              ~blobs_failed:state.blobs_failed
          else
            let%lwt imported, failed =
              Ops.import_blobs_batch ~pds_endpoint:state.old_pds
                ~access_jwt:state.access_jwt ~did:state.did ~cids:missing_cids
            in
            let new_imported = state.blobs_imported + imported in
            let new_failed = state.blobs_failed + failed in
            let new_cursor = Option.value ~default:"" next_cursor in
            if String.length new_cursor = 0 then
              transition_to_plc_token_step ctx ~did:state.did
                ~handle:state.handle ~old_pds:state.old_pds
                ~access_jwt:state.access_jwt ~refresh_jwt:state.refresh_jwt
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
                     ~step:"importing_data" ~did:state.did ~handle:state.handle
                     ~old_pds:state.old_pds ~blobs_imported:new_imported
                     ~blobs_failed:new_failed () ) )

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
        let%lwt old_pds_keys =
          match%lwt
            Remote.get_recommended_credentials ~pds_endpoint:state.old_pds
              ~access_jwt:state.access_jwt
          with
          | Ok creds ->
              Lwt.return creds.rotation_keys
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
        match%lwt
          Ops.get_recommended_did_credentials state.did ctx.db
            ~extra_rotation_keys:keys_to_preserve
        with
        | Error e ->
            render_err ~step:"enter_plc_token" ~did:state.did
              ~handle:state.handle ~old_pds:state.old_pds
              ("Failed to get credentials: " ^ e)
        | Ok credentials -> (
          match%lwt
            Remote.sign_plc_operation ~pds_endpoint:state.old_pds
              ~access_jwt:state.access_jwt ~token:plc_token ~credentials
          with
          | Error e ->
              render_err ~step:"enter_plc_token" ~did:state.did
                ~handle:state.handle ~old_pds:state.old_pds
                ("Failed to sign PLC operation: " ^ e)
          | Ok signed_operation -> (
            match%lwt
              Ops.submit_plc_operation ~did:state.did ~handle:state.handle
                ~operation:signed_operation ctx.db
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
                  deactivate_old_account_with_refresh ~old_pds:state.old_pds
                    ~access_jwt:state.access_jwt ~refresh_jwt:state.refresh_jwt
                in
                let old_account_deactivated, old_account_deactivation_error =
                  match deactivation_result with
                  | Ok () ->
                      (true, None)
                  | Error e ->
                      Dream.warning (fun log ->
                          log
                            "migration %s: failed to deactivate old account: %s"
                            state.did e ) ;
                      (false, Some e)
                in
                Util.render_html ~title:"Migrate Account"
                  (module Frontend.MigratePage)
                  ~props:
                    (make_props ~csrf_token ~invite_required ~hostname
                       ~step:"complete" ~did:state.did ~handle:state.handle
                       ~blobs_imported:state.blobs_imported
                       ~blobs_failed:state.blobs_failed ~old_account_deactivated
                       ?old_account_deactivation_error
                       ~message:"Your account has been successfully migrated!"
                       () ) ) ) )

and handle_resend_plc_token (ctx : Xrpc.context) ~csrf_token ~invite_required
    ~hostname =
  match State.get ctx.req with
  | None ->
      render_error ~csrf_token ~invite_required ~hostname
        "Migration state not found. Please start over."
  | Some state -> (
    match%lwt
      Remote.request_plc_signature ~pds_endpoint:state.old_pds
        ~access_jwt:state.access_jwt
    with
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
               ~message:"Confirmation code resent! Check your email." () ) )

and handle_submit_2fa (ctx : Xrpc.context) ~(render_err : render_err) fields =
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
          Remote.create_session ~pds_endpoint ~identifier ~password
            ~auth_factor_token ()
        with
        | Remote.AuthError e ->
            render_err ~step:"enter_2fa" ~identifier ~old_pds:pds_endpoint
              ?invite_code e
        | Remote.AuthNeeds2FA ->
            render_err ~step:"enter_2fa" ~identifier ~old_pds:pds_endpoint
              ?invite_code "Invalid authentication code. Please try again."
        | Remote.AuthSuccess session -> (
          match%lwt
            Remote.get_session ~pds_endpoint ~access_jwt:session.access_jwt
          with
          | Error e ->
              render_err ("Failed to get account info: " ^ e)
          | Ok session_info -> (
              let is_active =
                match session_info.active with Some false -> false | _ -> true
              in
              if not is_active then
                render_err
                  "This account is already deactivated. Cannot migrate a \
                   deactivated account."
              else
                match%lwt
                  Remote.get_service_auth ~pds_endpoint
                    ~access_jwt:session.access_jwt
                with
                | Error e ->
                    render_err ("Failed to get service authorization: " ^ e)
                | Ok service_auth_token -> (
                    let email =
                      match session_info.email with
                      | Some e when String.length e > 0 ->
                          e
                      | _ ->
                          Printf.sprintf "%s@%s" did Env.hostname
                    in
                    match%lwt
                      Ops.create_account ~email ~handle ~password ~did
                        ~service_auth_token ?invite_code ctx.db
                    with
                    | Error e ->
                        render_err e
                    | Ok _signing_key_did -> (
                      match%lwt
                        Remote.fetch_repo ~pds_endpoint
                          ~access_jwt:session.access_jwt ~did
                      with
                      | Error e ->
                          render_err ("Failed to fetch repository: " ^ e)
                      | Ok car_data -> (
                        match%lwt Ops.import_repo ~did ~car_data with
                        | Error e ->
                            render_err e
                        | Ok () ->
                            transition_to_plc_token_step ctx ~did ~handle
                              ~old_pds:pds_endpoint
                              ~access_jwt:session.access_jwt
                              ~refresh_jwt:session.refresh_jwt ~email
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
        Remote.create_session ~pds_endpoint:old_pds ~identifier ~password ()
      with
      | Remote.AuthError e ->
          render_err ~step:"resume_available" e
      | Remote.AuthNeeds2FA ->
          Util.render_html ~title:"Migrate Account"
            (module Frontend.MigratePage)
            ~props:
              (make_props ~csrf_token ~invite_required ~hostname
                 ~step:"enter_2fa" ~identifier ~old_pds () )
      | Remote.AuthSuccess session -> (
          let%lwt email =
            match%lwt
              Remote.get_session ~pds_endpoint:old_pds
                ~access_jwt:session.access_jwt
            with
            | Ok info ->
                Lwt.return
                  ( match info.email with
                  | Some e when String.length e > 0 ->
                      e
                  | _ ->
                      Printf.sprintf "%s@%s" did Env.hostname )
            | Error _ ->
                Lwt.return (Printf.sprintf "%s@%s" did Env.hostname)
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
                deactivate_old_account_with_refresh ~old_pds
                  ~access_jwt:session.access_jwt
                  ~refresh_jwt:session.refresh_jwt
              in
              let old_account_deactivated, old_account_deactivation_error =
                match deactivation_result with
                | Ok () ->
                    (true, None)
                | Error e ->
                    Dream.warning (fun log ->
                        log "migration %s: failed to deactivate old account: %s"
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
              transition_to_plc_token_step ctx ~did ~handle ~old_pds
                ~access_jwt:session.access_jwt ~refresh_jwt:session.refresh_jwt
                ~email ~blobs_imported:0 ~blobs_failed:0
          | Ok State.NeedsRepoImport | Ok State.NeedsBlobImport -> (
            match%lwt
              Remote.fetch_repo ~pds_endpoint:old_pds
                ~access_jwt:session.access_jwt ~did
            with
            | Error e ->
                render_err ~did ~handle ~old_pds
                  ("Failed to fetch repository: " ^ e)
            | Ok car_data -> (
              match%lwt Ops.import_repo ~did ~car_data with
              | Error e ->
                  render_err ~did ~handle ~old_pds e
              | Ok () -> (
                match%lwt Ops.list_missing_blobs ~did ~limit:50 () with
                | Error e ->
                    Dream.warning (fun log ->
                        log "migration %s: failed to list missing blobs: %s" did
                          e ) ;
                    transition_to_plc_token_step ctx ~did ~handle ~old_pds
                      ~access_jwt:session.access_jwt
                      ~refresh_jwt:session.refresh_jwt ~email ~blobs_imported:0
                      ~blobs_failed:0
                | Ok (missing_cids, next_cursor) ->
                    if List.length missing_cids = 0 then
                      transition_to_plc_token_step ctx ~did ~handle ~old_pds
                        ~access_jwt:session.access_jwt
                        ~refresh_jwt:session.refresh_jwt ~email
                        ~blobs_imported:0 ~blobs_failed:0
                    else
                      let%lwt imported, failed =
                        Ops.import_blobs_batch ~pds_endpoint:old_pds
                          ~access_jwt:session.access_jwt ~did ~cids:missing_cids
                      in
                      let cursor = Option.value ~default:"" next_cursor in
                      if String.length cursor = 0 then
                        transition_to_plc_token_step ctx ~did ~handle ~old_pds
                          ~access_jwt:session.access_jwt
                          ~refresh_jwt:session.refresh_jwt ~email
                          ~blobs_imported:imported ~blobs_failed:failed
                      else
                        let%lwt () =
                          State.set ctx.req
                            { did
                            ; handle
                            ; old_pds
                            ; access_jwt= session.access_jwt
                            ; refresh_jwt= session.refresh_jwt
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
                               ~blobs_imported:imported ~blobs_failed:failed () )
                ) ) ) ) )

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
              handle_submit_2fa ctx ~render_err fields
          | "resume_migration" ->
              handle_resume_migration ctx ~csrf_token ~invite_required ~hostname
                ~render_err fields
          | _ ->
              render_err "Invalid action" )
      | _ ->
          render_err "Invalid form submission" )
