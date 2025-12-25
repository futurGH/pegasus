let has_valid_plc_code (actor : Data_store.Types.actor) =
  match (actor.auth_code, actor.auth_code_expires_at) with
  | Some code, Some expires_at ->
      String.starts_with ~prefix:"plc-" code && expires_at > Util.now_ms ()
  | _ ->
      false

type dns_verification =
  { record: string
  ; value: string option
  ; valid_record: bool
  ; valid_did: bool
  ; resolvers_synced: bool
  ; error: string option }

type http_verification =
  { endpoint: string
  ; value: string option
  ; valid_endpoint: bool
  ; valid_did: bool
  ; error: string option }

type handle_verification =
  { handle: string
  ; did: string
  ; passed: bool
  ; method_: string option
  ; dns: dns_verification
  ; http: http_verification }

let verify_handle ~handle ~did =
  (* Verify via DNS *)
  let dns_record = "_atproto." ^ handle in
  let%lwt dns_result = Id_resolver.Handle.resolve_dns handle in
  let dns_value, dns_valid_record, dns_valid_did, dns_resolvers_synced, dns_error
      =
    match dns_result with
    | Ok resolved_did ->
        let is_valid_did =
          String.starts_with ~prefix:"did:plc:" resolved_did
          || String.starts_with ~prefix:"did:web:" resolved_did
        in
        let matches_did = resolved_did = did in
        ( Some ("did=" ^ resolved_did)
        , matches_did
        , is_valid_did
        , matches_did
        , None )
    | Error e ->
        (None, false, false, false, Some e)
  in
  (* Verify via HTTP/.well-known *)
  let http_endpoint = "https://" ^ handle ^ "/.well-known/atproto-did" in
  let%lwt http_result = Id_resolver.Handle.resolve_well_known handle in
  let http_value, http_valid_endpoint, http_valid_did, http_error =
    match http_result with
    | Ok resolved_did ->
        let is_valid_did =
          String.starts_with ~prefix:"did:plc:" resolved_did
          || String.starts_with ~prefix:"did:web:" resolved_did
        in
        let matches_did = resolved_did = did in
        (Some resolved_did, matches_did, is_valid_did, None)
    | Error e ->
        (None, false, false, Some e)
  in
  let dns_passed = dns_valid_record && dns_valid_did && dns_resolvers_synced in
  let http_passed = http_valid_endpoint && http_valid_did in
  let passed = dns_passed || http_passed in
  let method_ =
    match (dns_passed, http_passed) with
    | true, true ->
        Some "both"
    | true, false ->
        Some "dns"
    | false, true ->
        Some "http"
    | false, false ->
        None
  in
  Lwt.return
    { handle
    ; did
    ; passed
    ; method_
    ; dns=
        { record= dns_record
        ; value= dns_value
        ; valid_record= dns_valid_record
        ; valid_did= dns_valid_did
        ; resolvers_synced= dns_resolvers_synced
        ; error= dns_error }
    ; http=
        { endpoint= http_endpoint
        ; value= http_value
        ; valid_endpoint= http_valid_endpoint
        ; valid_did= http_valid_did
        ; error= http_error } }

type rotation_key = {key: string; is_pds_key: bool}

type plc_service = {id: string; type_: string; endpoint: string}

type verification_method = {method_id: string; method_key: string}

type plc_state =
  { rotation_keys: rotation_key list
  ; verification_methods: verification_method list
  ; also_known_as: string list
  ; services: plc_service list }

let get_plc_state ~did =
  let pds_pubkey =
    Env.rotation_key |> Kleidos.derive_pubkey |> Kleidos.pubkey_to_did_key
  in
  match%lwt Plc.get_audit_log did with
  | Ok log -> (
    match Mist.Util.last log with
    | Some latest ->
        let rotation_keys =
          List.map
            (fun key -> {key; is_pds_key= key = pds_pubkey})
            latest.operation.rotation_keys
        in
        let verification_methods =
          List.map
            (fun (id, key) -> {method_id= id; method_key= key})
            latest.operation.verification_methods
        in
        let also_known_as = latest.operation.also_known_as in
        let services =
          List.map
            (fun (id, (svc : Plc.service)) ->
              {id; type_= svc.type'; endpoint= svc.endpoint} )
            latest.operation.services
        in
        Lwt.return_ok {rotation_keys; verification_methods; also_known_as; services}
    | None ->
        Lwt.return_error "No operations found in audit log" )
  | Error e ->
      Lwt.return_error ("Failed to get PLC audit log: " ^ e)

let get_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Dream.redirect ctx.req "/account/login"
      | Some did -> (
          (* Only allow did:plc users *)
          if not (String.starts_with ~prefix:"did:plc:" did) then
            Dream.redirect ctx.req "/account"
          else
            let%lwt current_user, logged_in_users =
              Session.list_logged_in_actors ctx.req ctx.db
            in
            match%lwt Data_store.get_actor_by_identifier did ctx.db with
            | None ->
                Dream.redirect ctx.req "/account/login"
            | Some actor ->
                let current_user =
                  Option.value
                    ~default:
                      {did= actor.did; handle= actor.handle; avatar_data_uri= None}
                    current_user
                in
                let csrf_token = Dream.csrf_token ctx.req in
                let has_plc_token = has_valid_plc_code actor in
                let%lwt handle_verification =
                  verify_handle ~handle:actor.handle ~did
                in
                let%lwt plc_state_result = get_plc_state ~did in
                let plc_state =
                  match plc_state_result with
                  | Ok state ->
                      state
                  | Error _ ->
                      { rotation_keys= []
                      ; verification_methods= []
                      ; also_known_as= []
                      ; services= [] }
                in
                let error =
                  match plc_state_result with
                  | Error e ->
                      Some e
                  | Ok _ ->
                      None
                in
                Util.render_html ~title:"Identity"
                  (module Frontend.AccountIdentityPage)
                  ~props:
                    { current_user
                    ; logged_in_users
                    ; csrf_token
                    ; handle_verification=
                        { handle= handle_verification.handle
                        ; did= handle_verification.did
                        ; passed= handle_verification.passed
                        ; method_= handle_verification.method_
                        ; dns=
                            { record= handle_verification.dns.record
                            ; value= handle_verification.dns.value
                            ; valid_record= handle_verification.dns.valid_record
                            ; valid_did= handle_verification.dns.valid_did
                            ; resolvers_synced=
                                handle_verification.dns.resolvers_synced
                            ; error= handle_verification.dns.error }
                        ; http=
                            { endpoint= handle_verification.http.endpoint
                            ; value= handle_verification.http.value
                            ; valid_endpoint=
                                handle_verification.http.valid_endpoint
                            ; valid_did= handle_verification.http.valid_did
                            ; error= handle_verification.http.error } }
                    ; plc_state=
                        { rotation_keys=
                            List.map
                              (fun k ->
                                ( { key= k.key; is_pds_key= k.is_pds_key }
                                  : Frontend.AccountIdentityPage.rotation_key ))
                              plc_state.rotation_keys
                        ; verification_methods=
                            List.map
                              (fun vm ->
                                ( { method_id= vm.method_id
                                  ; method_key= vm.method_key }
                                  : Frontend.AccountIdentityPage.verification_method ))
                              plc_state.verification_methods
                        ; also_known_as= plc_state.also_known_as
                        ; services=
                            List.map
                              (fun s ->
                                ( { id= s.id
                                  ; type_= s.type_
                                  ; endpoint= s.endpoint }
                                  : Frontend.AccountIdentityPage.plc_service ))
                              plc_state.services }
                    ; has_plc_token
                    ; plc_token_error= None
                    ; plc_submit_error= None
                    ; plc_submit_success= None
                    ; error
                    ; success= None } ) )

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Dream.redirect ctx.req "/account/login"
      | Some did -> (
          (* Only allow did:plc users *)
          if not (String.starts_with ~prefix:"did:plc:" did) then
            Dream.redirect ctx.req "/account"
          else
            let%lwt current_user, logged_in_users =
              Session.list_logged_in_actors ctx.req ctx.db
            in
            match%lwt Data_store.get_actor_by_identifier did ctx.db with
            | None ->
                Dream.redirect ctx.req "/account/login"
            | Some actor -> (
                let current_user =
                  Option.value
                    ~default:
                      {did= actor.did; handle= actor.handle; avatar_data_uri= None}
                    current_user
                in
                let csrf_token = Dream.csrf_token ctx.req in
                let render_page ?plc_token_error ?plc_submit_error
                    ?plc_submit_success ?error ?success () =
                  let%lwt actor_opt =
                    Data_store.get_actor_by_identifier did ctx.db
                  in
                  let actor = Option.get actor_opt in
                  let has_plc_token = has_valid_plc_code actor in
                  let%lwt handle_verification =
                    verify_handle ~handle:actor.handle ~did
                  in
                  let%lwt plc_state_result = get_plc_state ~did in
                  let plc_state =
                    match plc_state_result with
                    | Ok state ->
                        state
                    | Error _ ->
                        { rotation_keys= []
                        ; verification_methods= []
                        ; also_known_as= []
                        ; services= [] }
                  in
                  let error =
                    match (error, plc_state_result) with
                    | Some e, _ ->
                        Some e
                    | None, Error e ->
                        Some e
                    | None, Ok _ ->
                        None
                  in
                  Util.render_html ~title:"Identity"
                    (module Frontend.AccountIdentityPage)
                    ~props:
                      { current_user= {current_user with handle= actor.handle}
                      ; logged_in_users
                      ; csrf_token
                      ; handle_verification=
                          { handle= handle_verification.handle
                          ; did= handle_verification.did
                          ; passed= handle_verification.passed
                          ; method_= handle_verification.method_
                          ; dns=
                              { record= handle_verification.dns.record
                              ; value= handle_verification.dns.value
                              ; valid_record= handle_verification.dns.valid_record
                              ; valid_did= handle_verification.dns.valid_did
                              ; resolvers_synced=
                                  handle_verification.dns.resolvers_synced
                              ; error= handle_verification.dns.error }
                          ; http=
                              { endpoint= handle_verification.http.endpoint
                              ; value= handle_verification.http.value
                              ; valid_endpoint=
                                  handle_verification.http.valid_endpoint
                              ; valid_did= handle_verification.http.valid_did
                              ; error= handle_verification.http.error } }
                      ; plc_state=
                          { rotation_keys=
                              List.map
                                (fun k ->
                                  ( { key= k.key; is_pds_key= k.is_pds_key }
                                    : Frontend.AccountIdentityPage.rotation_key ))
                                plc_state.rotation_keys
                          ; verification_methods=
                              List.map
                                (fun vm ->
                                  ( { method_id= vm.method_id
                                    ; method_key= vm.method_key }
                                    : Frontend.AccountIdentityPage.verification_method ))
                                plc_state.verification_methods
                          ; also_known_as= plc_state.also_known_as
                          ; services=
                              List.map
                                (fun s ->
                                  ( { id= s.id
                                    ; type_= s.type_
                                    ; endpoint= s.endpoint }
                                    : Frontend.AccountIdentityPage.plc_service ))
                                plc_state.services }
                      ; has_plc_token
                      ; plc_token_error
                      ; plc_submit_error
                      ; plc_submit_success
                      ; error
                      ; success }
                in
                match%lwt Dream.form ctx.req with
                | `Ok fields -> (
                    let action = List.assoc_opt "action" fields in
                    match action with
                    | Some "request_plc_token" ->
                        (* Request a PLC operation signature token *)
                        let code =
                          "plc-"
                          ^ String.sub
                              Digestif.SHA256.(
                                digest_string
                                  (did ^ Int.to_string @@ Util.now_ms ())
                                |> to_hex )
                              0 8
                        in
                        let expires_at = Util.now_ms () + (60 * 60 * 1000) in
                        let%lwt () =
                          Data_store.set_auth_code ~did ~code ~expires_at ctx.db
                        in
                        let%lwt () =
                          Util.send_email_or_log
                            ~recipients:[To actor.email]
                            ~subject:"Confirm PLC operation"
                            ~body:
                              (Plain
                                 (Printf.sprintf
                                    "Confirm that you would like to update \
                                     your PLC identity for %s (%s) using the \
                                     following token: %s"
                                    actor.handle did code ) )
                        in
                        Dream.empty `OK
                    | Some "sign_plc_operation" -> (
                        let token =
                          List.assoc_opt "token" fields
                          |> Option.value ~default:"" |> String.trim
                        in
                        match (actor.auth_code, actor.auth_code_expires_at) with
                        | Some auth_code, Some auth_expires_at
                          when String.starts_with ~prefix:"plc-" auth_code
                               && token = auth_code
                               && Util.now_ms () < auth_expires_at -> (
                          match%lwt Plc.get_audit_log did with
                          | Ok log -> (
                              let latest = Mist.Util.last log |> Option.get in
                              (* Create an operation that maintains current state *)
                              let unsigned_op : Plc.unsigned_operation =
                                Operation
                                  { type'= "plc_operation"
                                  ; rotation_keys= latest.operation.rotation_keys
                                  ; verification_methods=
                                      latest.operation.verification_methods
                                  ; also_known_as=
                                      latest.operation.also_known_as
                                  ; services= latest.operation.services
                                  ; prev= Some latest.cid }
                              in
                              let signed_op =
                                Plc.sign_operation Env.rotation_key unsigned_op
                              in
                              let%lwt () =
                                Data_store.clear_auth_code ~did ctx.db
                              in
                              match%lwt
                                Plc.submit_operation did signed_op
                              with
                              | Ok () ->
                                  let%lwt _ =
                                    Sequencer.sequence_identity ctx.db ~did ()
                                  in
                                  let%lwt _ =
                                    Id_resolver.Did.resolve ~skip_cache:true did
                                  in
                                  Dream.empty `OK
                              | Error (status, msg) ->
                                  Dream.respond ~status:`Bad_Request
                                    ( "Failed to submit: " ^ Int.to_string status
                                    ^ " " ^ msg ) )
                          | Error err ->
                              Dream.respond ~status:`Bad_Request
                                ("Failed to get audit log: " ^ err) )
                        | _ ->
                            Dream.respond ~status:`Bad_Request
                              "Invalid or expired token" )
                    | Some "cancel_plc_token" ->
                        let%lwt () = Data_store.clear_auth_code ~did ctx.db in
                        Dream.empty `OK
                    | _ ->
                        render_page ~error:"Invalid action." () )
                | _ ->
                    render_page ~error:"Invalid form submission." () ) ) )
