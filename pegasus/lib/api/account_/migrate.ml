open Cohttp_lwt

(* there are other properties but we don't need them *)
type create_session_response =
  { access_jwt: string [@key "accessJwt"]
  ; refresh_jwt: string [@key "refreshJwt"]
  ; handle: string
  ; did: string }
[@@deriving yojson {strict= false}]

type service_auth_response = {token: string} [@@deriving yojson {strict= false}]

type list_blobs_response =
  {cids: string list; cursor: string option [@default None]}
[@@deriving yojson {strict= false}]

type get_preferences_response = {preferences: Yojson.Safe.t list}
[@@deriving yojson {strict= false}]

type sign_plc_operation_response = {operation: Plc.signed_operation}
[@@deriving yojson {strict= false}]

type migration_state =
  { did: string
  ; handle: string
  ; old_pds: string
  ; access_jwt: string
  ; blobs_imported: int
  ; blobs_total: int
  ; blobs_failed: int
  ; blobs_cursor: string
  ; plc_requested: bool }
[@@deriving yojson]

let state_key = "migration_state"

let get_migration_state req =
  match Dream.session_field req state_key with
  | Some json -> (
    match migration_state_of_yojson (Yojson.Safe.from_string json) with
    | Ok state ->
        Some state
    | Error _ ->
        None )
  | None ->
      None

let set_migration_state req state =
  Dream.set_session_field req state_key
    (Yojson.Safe.to_string (migration_state_to_yojson state))

let clear_migration_state req = Dream.drop_session_field req state_key

(* HTTP helpers for communicating with old PDS *)
let post_json ~uri ~headers ~body =
  let headers = Http.Header.add headers "Content-Type" "application/json" in
  Cohttp_lwt_unix.Client.post ~headers
    ~body:(Body.of_string (Yojson.Safe.to_string body))
    uri

let post_empty ~uri ~headers =
  Cohttp_lwt_unix.Client.post ~headers ~body:Body.empty uri

let get_json ~uri ~headers = Util.http_get ~headers uri

(* Resolve handle to DID and find PDS endpoint *)
let resolve_identity identifier =
  let%lwt did =
    if String.starts_with ~prefix:"did:" identifier then
      Lwt.return_ok identifier
    else
      match%lwt Id_resolver.Handle.resolve identifier with
      | Ok did ->
          Lwt.return_ok did
      | Error e ->
          Lwt.return_error ("Failed to resolve handle: " ^ e)
  in
  match did with
  | Error e ->
      Lwt.return_error e
  | Ok did -> (
    match%lwt Id_resolver.Did.resolve did with
    | Error e ->
        Lwt.return_error ("Failed to resolve DID document: " ^ e)
    | Ok doc -> (
      match Id_resolver.Did.Document.get_service doc "#atproto_pds" with
      | None ->
          Lwt.return_error "No PDS service found in DID document"
      | Some pds_endpoint ->
          (* Get handle from alsoKnownAs *)
          let handle =
            match doc.also_known_as with
            | Some akas ->
                List.find_map
                  (fun aka ->
                    if String.starts_with ~prefix:"at://" aka then
                      Some (String.sub aka 5 (String.length aka - 5))
                    else None )
                  akas
                |> Option.value ~default:did
            | None ->
                did
          in
          Lwt.return_ok (did, handle, pds_endpoint) ) )

(* Authenticate with old PDS and get session *)
let authenticate_old_pds ~pds_endpoint ~identifier ~password =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.server.createSession"
  in
  let body =
    `Assoc [("identifier", `String identifier); ("password", `String password)]
  in
  let headers = Http.Header.init () in
  try%lwt
    let%lwt res, body = post_json ~uri ~headers ~body in
    match res.status with
    | `OK -> (
        let%lwt body_str = Body.to_string body in
        match
          create_session_response_of_yojson (Yojson.Safe.from_string body_str)
        with
        | Ok session ->
            Lwt.return_ok session
        | Error e ->
            Lwt.return_error ("Invalid session response: " ^ e) )
    | `Unauthorized ->
        let%lwt () = Body.drain_body body in
        Lwt.return_error "Invalid credentials"
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return_error
          (Printf.sprintf "Authentication failed (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* Get service auth token from old PDS for migration *)
let get_service_auth ~pds_endpoint ~access_jwt =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.server.getServiceAuth"
    |> fun u ->
    Uri.add_query_params' u
      [ ("aud", Env.did)
      ; ("lxm", "com.atproto.server.createAccount")
      ; ("exp", string_of_int (int_of_float (Unix.gettimeofday ()) + 300)) ]
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = get_json ~uri ~headers in
    match res.status with
    | `OK -> (
        let%lwt body_str = Body.to_string body in
        match
          service_auth_response_of_yojson (Yojson.Safe.from_string body_str)
        with
        | Ok {token} ->
            Lwt.return_ok token
        | Error e ->
            Lwt.return_error ("Invalid service auth response: " ^ e) )
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return_error
          (Printf.sprintf "Failed to get service auth (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* Request PLC operation signature from old PDS - sends email to user *)
let request_plc_operation_signature ~pds_endpoint ~access_jwt =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.identity.requestPlcOperationSignature"
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = post_empty ~uri ~headers in
    match res.status with
    | `OK ->
        let%lwt () = Body.drain_body body in
        Lwt.return_ok ()
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return_error
          (Printf.sprintf "Failed to request PLC signature (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* Sign PLC operation on old PDS with the token from email *)
let sign_plc_operation ~pds_endpoint ~access_jwt ~token
    ~(credentials : Plc.credentials) =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.identity.signPlcOperation"
  in
  let headers =
    Http.Header.of_list
      [ ("Authorization", "Bearer " ^ access_jwt)
      ; ("Content-Type", "application/json") ]
  in
  let body =
    `Assoc
      [ ("token", `String token)
      ; ("rotationKeys", `List (List.map (fun s -> `String s) credentials.rotation_keys))
      ; ( "verificationMethods"
        , `Assoc (List.map (fun (k, v) -> (k, `String v)) credentials.verification_methods) )
      ; ("alsoKnownAs", `List (List.map (fun s -> `String s) credentials.also_known_as))
      ; ("services", Plc.service_map_to_yojson credentials.services) ]
  in
  try%lwt
    let%lwt res, body =
      Cohttp_lwt_unix.Client.post ~headers
        ~body:(Body.of_string (Yojson.Safe.to_string body))
        uri
    in
    match res.status with
    | `OK -> (
        let%lwt body_str = Body.to_string body in
        match
          sign_plc_operation_response_of_yojson (Yojson.Safe.from_string body_str)
        with
        | Ok resp ->
            Lwt.return_ok resp.operation
        | Error e ->
            Lwt.return_error ("Invalid sign operation response: " ^ e) )
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return_error
          (Printf.sprintf "Failed to sign PLC operation (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* Get recommended DID credentials for this PDS *)
let get_recommended_did_credentials ~did ~handle db =
  match%lwt Data_store.get_actor_by_identifier did db with
  | None ->
      Lwt.return_error "Actor not found"
  | Some actor ->
      let signing_did_key =
        actor.signing_key |> Kleidos.parse_multikey_str
        |> Kleidos.derive_pubkey |> Kleidos.pubkey_to_did_key
      in
      Lwt.return_ok (Plc.create_did_credentials Env.rotation_key signing_did_key handle)

(* Submit signed PLC operation to PLC directory *)
let submit_plc_operation ~did ~(operation : Plc.signed_operation) db =
  (* Validate the operation before submitting *)
  let pds_pubkey =
    Env.rotation_key |> Kleidos.derive_pubkey |> Kleidos.pubkey_to_did_key
  in
  match operation with
  | Plc.Tombstone _ ->
      Lwt.return_error "Cannot submit tombstone operation during migration"
  | Plc.Operation op ->
      if not (List.mem pds_pubkey op.rotation_keys) then
        Lwt.return_error "Operation must include this PDS's rotation key"
      else (
        match List.assoc_opt "atproto_pds" op.services with
        | Some {endpoint; _} when endpoint <> Env.host_endpoint ->
            Lwt.return_error "Operation must point to this PDS"
        | None ->
            Lwt.return_error "Operation missing PDS service"
        | Some _ -> (
          match%lwt Plc.submit_operation did operation with
          | Ok () ->
              (* Sequence identity event and refresh cache *)
              let%lwt _ = Sequencer.sequence_identity db ~did () in
              let%lwt _ = Id_resolver.Did.resolve ~skip_cache:true did in
              Lwt.return_ok ()
          | Error (status, msg) ->
              Lwt.return_error
                (Printf.sprintf "PLC submission failed (%d): %s" status msg) ) )

(* Fetch repo CAR from old PDS *)
let fetch_repo ~pds_endpoint ~access_jwt ~did =
  let uri =
    Uri.with_path (Uri.of_string pds_endpoint) "/xrpc/com.atproto.sync.getRepo"
    |> fun u -> Uri.add_query_param' u ("did", did)
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = get_json ~uri ~headers in
    match res.status with
    | `OK ->
        let%lwt body_bytes = Body.to_string body in
        Lwt.return_ok (Bytes.of_string body_bytes)
    | status ->
        let%lwt () = Body.drain_body body in
        Lwt.return_error
          (Printf.sprintf "Failed to fetch repo (%s)"
             (Http.Status.to_string status) )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* List blobs from old PDS *)
let list_blobs ~pds_endpoint ~access_jwt ~did ?cursor () =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.sync.listBlobs"
    |> fun u ->
    Uri.add_query_param' u ("did", did)
    |> fun u ->
    match cursor with
    | Some c ->
        Uri.add_query_param' u ("cursor", c)
    | None ->
        u
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = get_json ~uri ~headers in
    match res.status with
    | `OK -> (
        let%lwt body_str = Body.to_string body in
        match
          list_blobs_response_of_yojson (Yojson.Safe.from_string body_str)
        with
        | Ok resp ->
            Lwt.return_ok resp
        | Error e ->
            Lwt.return_error ("Invalid list blobs response: " ^ e) )
    | status ->
        let%lwt () = Body.drain_body body in
        Lwt.return_error
          (Printf.sprintf "Failed to list blobs (%s)"
             (Http.Status.to_string status) )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* Fetch single blob from old PDS *)
let fetch_blob ~pds_endpoint ~access_jwt ~did ~cid =
  let uri =
    Uri.with_path (Uri.of_string pds_endpoint) "/xrpc/com.atproto.sync.getBlob"
    |> fun u -> Uri.add_query_params' u [("did", did); ("cid", cid)]
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = get_json ~uri ~headers in
    match res.status with
    | `OK ->
        let content_type =
          Http.Header.get res.headers "Content-Type"
          |> Option.value ~default:"application/octet-stream"
        in
        let%lwt body_bytes = Body.to_string body in
        Lwt.return_ok (content_type, Bytes.of_string body_bytes)
    | status ->
        let%lwt () = Body.drain_body body in
        Lwt.return_error
          (Printf.sprintf "Failed to fetch blob %s (%s)" cid
             (Http.Status.to_string status) )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* Fetch preferences from old PDS *)
let fetch_preferences ~pds_endpoint ~access_jwt =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/app.bsky.actor.getPreferences"
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = get_json ~uri ~headers in
    match res.status with
    | `OK -> (
        let%lwt body_str = Body.to_string body in
        match
          get_preferences_response_of_yojson (Yojson.Safe.from_string body_str)
        with
        | Ok resp ->
            Lwt.return_ok resp.preferences
        | Error e ->
            Dream.warning (fun log ->
                log "Failed to parse preferences response: %s" e ) ;
            Lwt.return_ok [] )
    | status ->
        let%lwt () = Body.drain_body body in
        Dream.warning (fun log ->
            log "Failed to fetch preferences: %s" (Http.Status.to_string status) ) ;
        Lwt.return_ok []
  with exn ->
    Dream.warning (fun log ->
        log "Exception fetching preferences: %s" (Printexc.to_string exn) ) ;
    Lwt.return_ok []

(* Create account on this PDS with existing DID *)
let create_migrated_account ~email ~handle ~password ~did ~service_auth_token
    ?invite_code db =
  let open Lwt.Infix in
  (* Verify service auth token - must be signed by the DID we're migrating *)
  let%lwt verified =
    match Jwt.decode_jwt service_auth_token with
    | Error e ->
        Lwt.return_error ("Invalid service auth token: " ^ e)
    | Ok (_header, payload) -> (
        let open Yojson.Safe.Util in
        try
          let iss = payload |> member "iss" |> to_string in
          let aud = payload |> member "aud" |> to_string in
          let lxm = payload |> member "lxm" |> to_string_option in
          let exp = payload |> member "exp" |> to_int in
          let now = int_of_float (Unix.gettimeofday ()) in
          (* Token must be issued by the DID (with optional fragment) *)
          let iss_did =
            match String.split_on_char '#' iss with
            | did :: _ ->
                did
            | [] ->
                iss
          in
          if iss_did <> did then
            Lwt.return_error "Service auth token issuer does not match DID"
          else if aud <> Env.did then
            Lwt.return_error
              "Service auth token audience does not match this PDS"
          else if lxm <> Some "com.atproto.server.createAccount" then
            Lwt.return_error
              "Service auth token not authorized for account creation"
          else if exp < now then Lwt.return_error "Service auth token expired"
          else
            (* Verify signature against DID document *)
            match%lwt Id_resolver.Did.resolve did with
            | Error e ->
                Lwt.return_error ("Failed to resolve DID: " ^ e)
            | Ok doc -> (
              match
                Id_resolver.Did.Document.get_verification_key doc "#atproto"
              with
              | None ->
                  Lwt.return_error "DID document missing atproto key"
              | Some pubkey_multibase -> (
                  let pubkey = Kleidos.parse_multikey_str pubkey_multibase in
                  match Jwt.verify_jwt service_auth_token ~pubkey with
                  | Ok _ ->
                      Lwt.return_ok ()
                  | Error e -> (
                    (* Try with fresh DID doc in case of key rotation *)
                    match%lwt
                      Id_resolver.Did.resolve ~skip_cache:true did
                    with
                    | Error _ ->
                        Lwt.return_error ("Invalid signature: " ^ e)
                    | Ok fresh_doc -> (
                      match
                        Id_resolver.Did.Document.get_verification_key fresh_doc
                          "#atproto"
                      with
                      | None ->
                          Lwt.return_error "DID document missing atproto key"
                      | Some fresh_pubkey_multibase -> (
                          let fresh_pubkey =
                            Kleidos.parse_multikey_str fresh_pubkey_multibase
                          in
                          match
                            Jwt.verify_jwt service_auth_token
                              ~pubkey:fresh_pubkey
                          with
                          | Ok _ ->
                              Lwt.return_ok ()
                          | Error e ->
                              Lwt.return_error ("Invalid signature: " ^ e) ) ) )
                  ) )
        with _ -> Lwt.return_error "Malformed service auth token" )
  in
  match verified with
  | Error e ->
      Lwt.return_error e
  | Ok () -> (
    (* Check if DID already exists *)
    match%lwt Data_store.get_actor_by_identifier did db with
    | Some _ ->
        Lwt.return_error "An account with this DID already exists on this PDS"
    | None -> (
      (* Check if handle is available (may need different handle) *)
      match%lwt Data_store.get_actor_by_identifier handle db with
      | Some _ ->
          Lwt.return_error
            ( "The handle @" ^ handle
            ^ " is already taken on this PDS. You may need to use a different \
               handle." )
      | None -> (
        (* Check if email is available *)
        match%lwt Data_store.get_actor_by_identifier email db with
        | Some _ ->
            Lwt.return_error "An account with this email already exists"
        | None -> (
            (* Validate invite code if required *)
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
                (* Generate new signing key for this PDS *)
                let signing_key, signing_pubkey =
                  Kleidos.K256.generate_keypair ()
                in
                let sk_priv_mk = Kleidos.K256.privkey_to_multikey signing_key in
                (* Create actor in deactivated state *)
                let%lwt () =
                  Data_store.create_actor ~did ~handle ~email ~password
                    ~signing_key:sk_priv_mk db
                in
                let%lwt () = Data_store.deactivate_actor did db in
                (* Create user database *)
                let () =
                  Util.mkfile_p
                    (Util.Constants.user_db_filepath did)
                    ~perm:0o644
                in
                (* Sequence identity and account events *)
                let%lwt _ = Sequencer.sequence_identity db ~did ~handle () in
                let%lwt _ =
                  Sequencer.sequence_account db ~did ~active:false
                    ~status:`Deactivated ()
                in
                Lwt.return_ok (Kleidos.K256.pubkey_to_did_key signing_pubkey) )
        ) ) )

(* Convert bytes to Lwt_seq stream for CAR import *)
let bytes_to_car_stream (data : bytes) : Car.stream =
 fun () -> Lwt.return (Lwt_seq.Cons (data, fun () -> Lwt.return Lwt_seq.Nil))

(* Import repo CAR data *)
let import_repo ~did ~car_data db =
  try%lwt
    let%lwt repo = Repository.load ~write:true ~create:true ~ds:db did in
    let stream = bytes_to_car_stream car_data in
    match%lwt Repository.import_car repo stream with
    | Ok _ ->
        Lwt.return_ok ()
    | Error e ->
        Lwt.return_error ("Failed to import repository: " ^ Printexc.to_string e)
  with exn ->
    Lwt.return_error ("Failed to import repository: " ^ Printexc.to_string exn)

(* Import blobs in batches *)
let import_blobs_batch ~pds_endpoint ~access_jwt ~did ~cids =
  let%lwt user_db = User_store.connect ~create:true ~write:true did in
  let%lwt results =
    Lwt_list.map_p
      (fun cid_str ->
        match%lwt fetch_blob ~pds_endpoint ~access_jwt ~did ~cid:cid_str with
        | Error e ->
            Dream.warning (fun log ->
                log "Failed to fetch blob %s: %s" cid_str e ) ;
            Lwt.return_error cid_str
        | Ok (mimetype, data) -> (
          match Cid.of_string cid_str with
          | Error _ ->
              Lwt.return_error cid_str
          | Ok cid ->
              let%lwt _ = User_store.put_blob user_db cid mimetype data in
              Lwt.return_ok cid_str ) )
      cids
  in
  let imported =
    List.filter (function Ok _ -> true | Error _ -> false) results
    |> List.length
  in
  let failed =
    List.filter (function Error _ -> true | Ok _ -> false) results
    |> List.length
  in
  Lwt.return (imported, failed)

(* Import preferences *)
let import_preferences ~did ~preferences db =
  if List.length preferences = 0 then Lwt.return_unit
  else
    let prefs = `List preferences in
    Data_store.put_preferences ~did ~prefs db

(* Normalize endpoint by removing trailing slash *)
let normalize_endpoint s =
  if String.length s > 0 && s.[String.length s - 1] = '/' then
    String.sub s 0 (String.length s - 1)
  else s

(* Check if PLC identity has been updated to point to this PDS *)
let check_identity_updated did =
  match%lwt Id_resolver.Did.resolve ~skip_cache:true did with
  | Error e ->
      Lwt.return_error ("Failed to resolve DID: " ^ e)
  | Ok doc -> (
    match Id_resolver.Did.Document.get_service doc "#atproto_pds" with
    | None ->
        Lwt.return_error "DID document missing PDS service"
    | Some endpoint ->
        let normalized_endpoint = normalize_endpoint endpoint in
        let normalized_host = normalize_endpoint Env.host_endpoint in
        if normalized_endpoint = normalized_host then Lwt.return_ok true
        else Lwt.return_ok false )

(* Activate the migrated account *)
let activate_account did db =
  let%lwt () = Data_store.activate_actor did db in
  let%lwt _ =
    Sequencer.sequence_account db ~did ~active:true ~status:`Active ()
  in
  Lwt.return_unit

(* GET handler - display the migrate page *)
let get_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let invite_required = Env.invite_required in
      let hostname = Env.hostname in
      (* Check for existing migration state *)
      let props : Frontend.MigratePage.props =
        match get_migration_state ctx.req with
        | None ->
            { csrf_token
            ; invite_required
            ; hostname
            ; step= "enter_credentials"
            ; did= None
            ; handle= None
            ; old_pds= None
            ; blobs_imported= 0
            ; blobs_total= 0
            ; blobs_failed= 0
            ; error= None
            ; message= None }
        | Some state ->
            if state.plc_requested then
              { csrf_token
              ; invite_required
              ; hostname
              ; step= "enter_plc_token"
              ; did= Some state.did
              ; handle= Some state.handle
              ; old_pds= Some state.old_pds
              ; blobs_imported= state.blobs_imported
              ; blobs_total= state.blobs_total
              ; blobs_failed= state.blobs_failed
              ; error= None
              ; message= None }
            else
              { csrf_token
              ; invite_required
              ; hostname
              ; step= "importing_blobs"
              ; did= Some state.did
              ; handle= Some state.handle
              ; old_pds= Some state.old_pds
              ; blobs_imported= state.blobs_imported
              ; blobs_total= state.blobs_total
              ; blobs_failed= state.blobs_failed
              ; error= None
              ; message= None }
      in
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props )

(* POST handler - process migration steps *)
let post_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let invite_required = Env.invite_required in
      let hostname = Env.hostname in
      let make_props ?(step = "enter_credentials") ?did ?handle ?old_pds
          ?(blobs_imported = 0) ?(blobs_total = 0) ?(blobs_failed = 0) ?error
          ?message () : Frontend.MigratePage.props =
        { csrf_token
        ; invite_required
        ; hostname
        ; step
        ; did
        ; handle
        ; old_pds
        ; blobs_imported
        ; blobs_total
        ; blobs_failed
        ; error
        ; message }
      in
      let render_error ?(step = "enter_credentials") ?did ?handle ?old_pds error
          =
        Util.render_html ~status:`Bad_Request ~title:"Migrate Account"
          (module Frontend.MigratePage)
          ~props:(make_props ~step ?did ?handle ?old_pds ~error () )
      in
      (* Helper to transition to PLC token step after data import *)
      let transition_to_plc_token_step ~did ~handle ~old_pds ~access_jwt
          ~blobs_imported ~blobs_total ~blobs_failed =
        (* Request PLC operation signature from old PDS *)
        match%lwt
          request_plc_operation_signature ~pds_endpoint:old_pds ~access_jwt
        with
        | Error e ->
            Dream.warning (fun log ->
                log "Failed to request PLC signature: %s" e ) ;
            (* Still show the token step, user may have received email already *)
            let%lwt () =
              set_migration_state ctx.req
                { did
                ; handle
                ; old_pds
                ; access_jwt
                ; blobs_imported
                ; blobs_total
                ; blobs_failed
                ; blobs_cursor= ""
                ; plc_requested= true }
            in
            Util.render_html ~title:"Migrate Account"
              (module Frontend.MigratePage)
              ~props:
                (make_props ~step:"enter_plc_token" ~did:(Some did)
                   ~handle:(Some handle) ~old_pds:(Some old_pds)
                   ~blobs_imported ~blobs_total ~blobs_failed
                   ~message:
                     (Some
                        "Data import complete! Check your email for a PLC \
                         confirmation code." )
                   ~error:
                     (Some
                        ( "Note: Could not automatically request PLC \
                           signature: " ^ e
                        ^ ". You may need to request it manually from your \
                           old PDS." ) )
                   () )
        | Ok () ->
            let%lwt () =
              set_migration_state ctx.req
                { did
                ; handle
                ; old_pds
                ; access_jwt
                ; blobs_imported
                ; blobs_total
                ; blobs_failed
                ; blobs_cursor= ""
                ; plc_requested= true }
            in
            Util.render_html ~title:"Migrate Account"
              (module Frontend.MigratePage)
              ~props:
                (make_props ~step:"enter_plc_token" ~did:(Some did)
                   ~handle:(Some handle) ~old_pds:(Some old_pds)
                   ~blobs_imported ~blobs_total ~blobs_failed
                   ~message:
                     (Some
                        "Data import complete! Check your email for a PLC \
                         confirmation code." )
                   () )
      in
      match%lwt Dream.form ctx.req with
      | `Ok fields -> (
          let action =
            List.assoc_opt "action" fields |> Option.value ~default:""
          in
          match action with
          | "start_migration" -> (
              let identifier =
                List.assoc_opt "identifier" fields
                |> Option.value ~default:"" |> String.trim
              in
              let password =
                List.assoc_opt "password" fields |> Option.value ~default:""
              in
              let invite_code =
                List.assoc_opt "invite_code" fields
                |> Option.map String.trim
                |> fun c ->
                Option.bind c (fun s ->
                    if String.length s = 0 then None else Some s )
              in
              if String.length identifier = 0 then
                render_error "Please enter your handle or DID"
              else if String.length password = 0 then
                render_error "Please enter your password"
              else
                (* Step 1: Resolve identity *)
                match%lwt resolve_identity identifier with
                | Error e ->
                    render_error e
                | Ok (did, handle, old_pds) -> (
                  (* Step 2: Authenticate with old PDS *)
                  match%lwt
                    authenticate_old_pds ~pds_endpoint:old_pds ~identifier
                      ~password
                  with
                  | Error e ->
                      render_error e
                  | Ok session -> (
                    (* Step 3: Get service auth token *)
                    match%lwt
                      get_service_auth ~pds_endpoint:old_pds
                        ~access_jwt:session.access_jwt
                    with
                    | Error e ->
                        render_error
                          ("Failed to get service authorization: " ^ e)
                    | Ok service_auth_token -> (
                        (* Use email placeholder - user can update later *)
                        let email = Printf.sprintf "%s@migrated.local" did in
                        (* Step 4: Create account *)
                        match%lwt
                          create_migrated_account ~email ~handle ~password ~did
                            ~service_auth_token ?invite_code ctx.db
                        with
                        | Error e ->
                            render_error e
                        | Ok _signing_key_did -> (
                          (* Step 5: Fetch and import repo *)
                          match%lwt
                            fetch_repo ~pds_endpoint:old_pds
                              ~access_jwt:session.access_jwt ~did
                          with
                          | Error e ->
                              render_error ("Failed to fetch repository: " ^ e)
                          | Ok car_data -> (
                            match%lwt import_repo ~did ~car_data ctx.db with
                            | Error e ->
                                render_error e
                            | Ok () -> (
                              (* Step 6: List blobs to import *)
                              match%lwt
                                list_blobs ~pds_endpoint:old_pds
                                  ~access_jwt:session.access_jwt ~did ()
                              with
                              | Error _ ->
                                  (* Blobs optional, continue to preferences *)
                                  let%lwt prefs =
                                    fetch_preferences ~pds_endpoint:old_pds
                                      ~access_jwt:session.access_jwt
                                  in
                                  let%lwt () =
                                    match prefs with
                                    | Ok p when List.length p > 0 ->
                                        import_preferences ~did ~preferences:p
                                          ctx.db
                                    | _ ->
                                        Lwt.return_unit
                                  in
                                  (* Go to PLC token step *)
                                  transition_to_plc_token_step ~did ~handle
                                    ~old_pds ~access_jwt:session.access_jwt
                                    ~blobs_imported:0 ~blobs_total:0
                                    ~blobs_failed:0
                              | Ok blob_list ->
                                  let total = List.length blob_list.cids in
                                  if total = 0 then
                                    (* No blobs, fetch preferences and go to PLC token *)
                                    let%lwt prefs =
                                      fetch_preferences ~pds_endpoint:old_pds
                                        ~access_jwt:session.access_jwt
                                    in
                                    let%lwt () =
                                      match prefs with
                                      | Ok p when List.length p > 0 ->
                                          import_preferences ~did ~preferences:p
                                            ctx.db
                                      | _ ->
                                          Lwt.return_unit
                                    in
                                    transition_to_plc_token_step ~did ~handle
                                      ~old_pds ~access_jwt:session.access_jwt
                                      ~blobs_imported:0 ~blobs_total:0
                                      ~blobs_failed:0
                                  else
                                    (* Start blob import *)
                                    let batch =
                                      List.filteri
                                        (fun i _ -> i < 50)
                                        blob_list.cids
                                    in
                                    let%lwt imported, failed =
                                      import_blobs_batch ~pds_endpoint:old_pds
                                        ~access_jwt:session.access_jwt ~did
                                        ~cids:batch
                                    in
                                    (* Store state for continuation *)
                                    let cursor =
                                      Option.value ~default:"" blob_list.cursor
                                    in
                                    let%lwt () =
                                      set_migration_state ctx.req
                                        { did
                                        ; handle
                                        ; old_pds
                                        ; access_jwt= session.access_jwt
                                        ; blobs_imported= imported
                                        ; blobs_total= total
                                        ; blobs_failed= failed
                                        ; blobs_cursor= cursor
                                        ; plc_requested= false }
                                    in
                                    if imported + failed >= total then
                                      (* All done, fetch preferences and go to PLC *)
                                      let%lwt prefs =
                                        fetch_preferences ~pds_endpoint:old_pds
                                          ~access_jwt:session.access_jwt
                                      in
                                      let%lwt () =
                                        match prefs with
                                        | Ok p when List.length p > 0 ->
                                            import_preferences ~did
                                              ~preferences:p ctx.db
                                        | _ ->
                                            Lwt.return_unit
                                      in
                                      transition_to_plc_token_step ~did ~handle
                                        ~old_pds ~access_jwt:session.access_jwt
                                        ~blobs_imported:imported ~blobs_total:total
                                        ~blobs_failed:failed
                                    else
                                      Util.render_html ~title:"Migrate Account"
                                        (module Frontend.MigratePage)
                                        ~props:
                                          (make_props ~step:"importing_blobs"
                                             ~did:(Some did)
                                             ~handle:(Some handle)
                                             ~old_pds:(Some old_pds)
                                             ~blobs_imported:imported
                                             ~blobs_total:total
                                             ~blobs_failed:failed () ) ) ) ) ) )
                  ) )
          | "continue_blobs" -> (
            match get_migration_state ctx.req with
            | None ->
                render_error "Migration state not found. Please start over."
            | Some state -> (
                (* Continue importing blobs *)
                let%lwt result =
                  list_blobs ~pds_endpoint:state.old_pds
                    ~access_jwt:state.access_jwt ~did:state.did
                    ~cursor:state.blobs_cursor ()
                in
                match result with
                | Error _ ->
                    (* No more blobs, fetch preferences and go to PLC token *)
                    let%lwt prefs =
                      fetch_preferences ~pds_endpoint:state.old_pds
                        ~access_jwt:state.access_jwt
                    in
                    let%lwt () =
                      match prefs with
                      | Ok p when List.length p > 0 ->
                          import_preferences ~did:state.did ~preferences:p
                            ctx.db
                      | _ ->
                          Lwt.return_unit
                    in
                    transition_to_plc_token_step ~did:state.did
                      ~handle:state.handle ~old_pds:state.old_pds
                      ~access_jwt:state.access_jwt
                      ~blobs_imported:state.blobs_imported
                      ~blobs_total:state.blobs_total
                      ~blobs_failed:state.blobs_failed
                | Ok blob_list ->
                    if List.length blob_list.cids = 0 then
                      let%lwt prefs =
                        fetch_preferences ~pds_endpoint:state.old_pds
                          ~access_jwt:state.access_jwt
                      in
                      let%lwt () =
                        match prefs with
                        | Ok p when List.length p > 0 ->
                            import_preferences ~did:state.did ~preferences:p
                              ctx.db
                        | _ ->
                            Lwt.return_unit
                      in
                      transition_to_plc_token_step ~did:state.did
                        ~handle:state.handle ~old_pds:state.old_pds
                        ~access_jwt:state.access_jwt
                        ~blobs_imported:state.blobs_imported
                        ~blobs_total:state.blobs_total
                        ~blobs_failed:state.blobs_failed
                    else
                      let batch =
                        List.filteri (fun i _ -> i < 50) blob_list.cids
                      in
                      let%lwt imported, failed =
                        import_blobs_batch ~pds_endpoint:state.old_pds
                          ~access_jwt:state.access_jwt ~did:state.did
                          ~cids:batch
                      in
                      let new_imported = state.blobs_imported + imported in
                      let new_failed = state.blobs_failed + failed in
                      let new_cursor =
                        match blob_list.cursor with
                        | Some c ->
                            c
                        | None ->
                            List.nth_opt batch (List.length batch - 1)
                            |> Option.value ~default:""
                      in
                      let%lwt () =
                        set_migration_state ctx.req
                          { state with
                            blobs_imported= new_imported
                          ; blobs_failed= new_failed
                          ; blobs_cursor= new_cursor }
                      in
                      if
                        new_imported + new_failed >= state.blobs_total
                        || List.length blob_list.cids < 50
                      then
                        let%lwt prefs =
                          fetch_preferences ~pds_endpoint:state.old_pds
                            ~access_jwt:state.access_jwt
                        in
                        let%lwt () =
                          match prefs with
                          | Ok p when List.length p > 0 ->
                              import_preferences ~did:state.did ~preferences:p
                                ctx.db
                          | _ ->
                              Lwt.return_unit
                        in
                        transition_to_plc_token_step ~did:state.did
                          ~handle:state.handle ~old_pds:state.old_pds
                          ~access_jwt:state.access_jwt
                          ~blobs_imported:new_imported
                          ~blobs_total:state.blobs_total
                          ~blobs_failed:new_failed
                      else
                        Util.render_html ~title:"Migrate Account"
                          (module Frontend.MigratePage)
                          ~props:
                            (make_props ~step:"importing_blobs"
                               ~did:(Some state.did) ~handle:(Some state.handle)
                               ~old_pds:(Some state.old_pds)
                               ~blobs_imported:new_imported
                               ~blobs_total:state.blobs_total
                               ~blobs_failed:new_failed () ) ) )
          | "submit_plc_token" -> (
            match get_migration_state ctx.req with
            | None ->
                render_error "Migration state not found. Please start over."
            | Some state ->
                let plc_token =
                  List.assoc_opt "plc_token" fields
                  |> Option.value ~default:"" |> String.trim
                in
                if String.length plc_token = 0 then
                  render_error ~step:"enter_plc_token" ~did:(Some state.did)
                    ~handle:(Some state.handle) ~old_pds:(Some state.old_pds)
                    "Please enter the PLC token from your email"
                else (
                  (* Get recommended credentials for this PDS *)
                  match%lwt
                    get_recommended_did_credentials ~did:state.did
                      ~handle:state.handle ctx.db
                  with
                  | Error e ->
                      render_error ~step:"enter_plc_token" ~did:(Some state.did)
                        ~handle:(Some state.handle) ~old_pds:(Some state.old_pds)
                        ("Failed to get credentials: " ^ e)
                  | Ok credentials -> (
                    (* Sign PLC operation on old PDS *)
                    match%lwt
                      sign_plc_operation ~pds_endpoint:state.old_pds
                        ~access_jwt:state.access_jwt ~token:plc_token
                        ~credentials
                    with
                    | Error e ->
                        render_error ~step:"enter_plc_token"
                          ~did:(Some state.did) ~handle:(Some state.handle)
                          ~old_pds:(Some state.old_pds)
                          ("Failed to sign PLC operation: " ^ e)
                    | Ok signed_operation -> (
                      (* Submit PLC operation *)
                      match%lwt
                        submit_plc_operation ~did:state.did
                          ~operation:signed_operation ctx.db
                      with
                      | Error e ->
                          render_error ~step:"enter_plc_token"
                            ~did:(Some state.did) ~handle:(Some state.handle)
                            ~old_pds:(Some state.old_pds)
                            ("Failed to submit PLC operation: " ^ e)
                      | Ok () ->
                          (* Activate the account *)
                          let%lwt () = activate_account state.did ctx.db in
                          let%lwt () = Session.log_in_did ctx.req state.did in
                          let%lwt () = clear_migration_state ctx.req in
                          Util.render_html ~title:"Migrate Account"
                            (module Frontend.MigratePage)
                            ~props:
                              (make_props ~step:"complete" ~did:(Some state.did)
                                 ~handle:(Some state.handle)
                                 ~blobs_imported:state.blobs_imported
                                 ~blobs_total:state.blobs_total
                                 ~blobs_failed:state.blobs_failed
                                 ~message:
                                   (Some
                                      "Your account has been successfully \
                                       migrated!" )
                                 () ) ) ) ) )
          | "resend_plc_token" -> (
            match get_migration_state ctx.req with
            | None ->
                render_error "Migration state not found. Please start over."
            | Some state -> (
              match%lwt
                request_plc_operation_signature ~pds_endpoint:state.old_pds
                  ~access_jwt:state.access_jwt
              with
              | Error e ->
                  Util.render_html ~title:"Migrate Account"
                    (module Frontend.MigratePage)
                    ~props:
                      (make_props ~step:"enter_plc_token" ~did:(Some state.did)
                         ~handle:(Some state.handle)
                         ~old_pds:(Some state.old_pds)
                         ~error:(Some ("Failed to resend: " ^ e))
                         () )
              | Ok () ->
                  Util.render_html ~title:"Migrate Account"
                    (module Frontend.MigratePage)
                    ~props:
                      (make_props ~step:"enter_plc_token" ~did:(Some state.did)
                         ~handle:(Some state.handle)
                         ~old_pds:(Some state.old_pds)
                         ~message:
                           (Some "Confirmation code resent! Check your email.")
                         () ) ) )
          | _ ->
              render_error "Invalid action" )
      | _ ->
          render_error "Invalid form submission" )
