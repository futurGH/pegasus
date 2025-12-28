type create_session_response = Server.CreateSession.response
[@@deriving yojson {strict= false}]

type get_session_response = Server.GetSession.response
[@@deriving yojson {strict= false}]

type refresh_session_response = Server.RefreshSession.response
[@@deriving yojson {strict= false}]

type service_auth_response = Server.GetServiceAuth.response
[@@deriving yojson {strict= false}]

type list_blobs_response = Repo.ListMissingBlobs.response
[@@deriving yojson {strict= false}]

type get_preferences_response = Proxy.AppBskyActorGetPreferences.response
[@@deriving yojson {strict= false}]

type sign_plc_operation_response = Identity.SignPlcOperation.response
[@@deriving yojson {strict= false}]

type check_account_status_response = Server.CheckAccountStatus.response
[@@deriving yojson {strict= false}]

type remote_credentials_response =
  Identity.GetRecommendedDidCredentials.response
[@@deriving yojson {strict= false}]

let get_account_status = Server.CheckAccountStatus.get_account_status

let get_recommended_did_credentials =
  Identity.GetRecommendedDidCredentials.get_credentials

open Cohttp_lwt

type migration_state =
  { did: string
  ; handle: string
  ; old_pds: string
  ; access_jwt: string
  ; refresh_jwt: string
  ; email: string
  ; blobs_imported: int
  ; blobs_failed: int
  ; blobs_cursor: string
  ; plc_requested: bool }
[@@deriving yojson]

let state_key = "pegasus.migration_state"

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

let post_json ~uri ~headers ~body =
  let headers = Http.Header.add headers "Content-Type" "application/json" in
  Cohttp_lwt_unix.Client.post ~headers
    ~body:(Body.of_string (Yojson.Safe.to_string body))
    uri

let post_empty ~uri ~headers =
  Cohttp_lwt_unix.Client.post ~headers ~body:Body.empty uri

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

type auth_result =
  | AuthSuccess of create_session_response
  | AuthNeeds2FA
  | AuthError of string

let create_session_on_pds ~pds_endpoint ~identifier ~password ?auth_factor_token
    () =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.server.createSession"
  in
  let body =
    let base =
      [("identifier", `String identifier); ("password", `String password)]
    in
    match auth_factor_token with
    | Some token ->
        `Assoc (("authFactorToken", `String token) :: base)
    | None ->
        `Assoc base
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
            Lwt.return (AuthSuccess session)
        | Error e ->
            Lwt.return (AuthError ("Invalid session response: " ^ e)) )
    | `Unauthorized -> (
        let%lwt body_str = Body.to_string body in
        (* Check if 2FA is required *)
        try
          let json = Yojson.Safe.from_string body_str in
          let open Yojson.Safe.Util in
          let error = json |> member "error" |> to_string_option in
          match error with
          | Some "AuthFactorTokenRequired" ->
              Lwt.return AuthNeeds2FA
          | _ ->
              Lwt.return (AuthError "Invalid credentials")
        with _ -> Lwt.return (AuthError "Invalid credentials") )
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return
          (AuthError
             (Printf.sprintf "Authentication failed (%s): %s"
                (Http.Status.to_string status)
                body_str ) )
  with exn ->
    Lwt.return (AuthError ("Network error: " ^ Printexc.to_string exn))

let get_session ~pds_endpoint ~access_jwt =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.server.getSession"
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = Util.http_get uri ~headers in
    match res.status with
    | `OK -> (
        let%lwt body_str = Body.to_string body in
        match
          get_session_response_of_yojson (Yojson.Safe.from_string body_str)
        with
        | Ok session ->
            Lwt.return_ok session
        | Error e ->
            Lwt.return_error ("Invalid session response: " ^ e) )
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return_error
          (Printf.sprintf "Failed to get session info (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* check if jwt is expired or will expire within delta_s *)
let jwt_needs_refresh ?(delta_s = 60) access_jwt =
  match Jwt.decode_jwt access_jwt with
  | Error _ ->
      true (* if we can't decode, assume we need refresh *)
  | Ok (_header, payload) -> (
    try
      let open Yojson.Safe.Util in
      let exp = payload |> member "exp" |> to_int in
      let now = int_of_float (Unix.gettimeofday ()) in
      exp - now < delta_s
    with _ -> true )

let refresh_session ~pds_endpoint ~refresh_jwt =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.server.refreshSession"
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ refresh_jwt)]
  in
  try%lwt
    let%lwt res, body = post_empty ~uri ~headers in
    match res.status with
    | `OK -> (
        let%lwt body_str = Body.to_string body in
        match
          refresh_session_response_of_yojson (Yojson.Safe.from_string body_str)
        with
        | Ok tokens ->
            Lwt.return_ok tokens
        | Error e ->
            Lwt.return_error ("Invalid refresh response: " ^ e) )
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return_error
          (Printf.sprintf "Failed to refresh session (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

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
    let%lwt res, body = Util.http_get uri ~headers in
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

(* get credentials from old pds so we can remove the pds rotation key *)
let get_remote_recommended_credentials ~pds_endpoint ~access_jwt =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.identity.getRecommendedDidCredentials"
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = Cohttp_lwt_unix.Client.get ~headers uri in
    match res.status with
    | `OK -> (
        let%lwt body_str = Body.to_string body in
        match
          remote_credentials_response_of_yojson
            (Yojson.Safe.from_string body_str)
        with
        | Ok creds ->
            Lwt.return_ok creds
        | Error e ->
            Lwt.return_error ("Invalid credentials response: " ^ e) )
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return_error
          (Printf.sprintf "Failed to get remote credentials (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* list current rotation keys from plc directory audit log *)
let get_plc_rotation_keys ~did =
  if not (String.starts_with ~prefix:"did:plc:" did) then Lwt.return_ok []
  else
    let uri =
      Uri.make ~scheme:"https" ~host:"plc.directory" ~path:(did ^ "/log/last")
        ()
    in
    try%lwt
      let%lwt res, body =
        Cohttp_lwt_unix.Client.get
          ~headers:(Http.Header.of_list [("Accept", "application/json")])
          uri
      in
      match res.status with
      | `OK -> (
          let%lwt body_str = Body.to_string body in
          try
            let json = Yojson.Safe.from_string body_str in
            let open Yojson.Safe.Util in
            let rotation_keys =
              json |> member "rotationKeys" |> to_list |> List.map to_string
            in
            Lwt.return_ok rotation_keys
          with _ -> Lwt.return_ok [] )
      | _ ->
          let%lwt () = Body.drain_body body in
          Lwt.return_ok []
    with _ -> Lwt.return_ok []

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
      ; ( "rotationKeys"
        , `List (List.map (fun s -> `String s) credentials.rotation_keys) )
      ; ( "verificationMethods"
        , `Assoc
            (List.map
               (fun (k, v) -> (k, `String v))
               credentials.verification_methods ) )
      ; ( "alsoKnownAs"
        , `List (List.map (fun s -> `String s) credentials.also_known_as) )
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
          sign_plc_operation_response_of_yojson
            (Yojson.Safe.from_string body_str)
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

let fetch_repo ~pds_endpoint ~access_jwt ~did =
  let uri =
    Uri.with_path (Uri.of_string pds_endpoint) "/xrpc/com.atproto.sync.getRepo"
    |> fun u -> Uri.add_query_param' u ("did", did)
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = Util.http_get uri ~headers in
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
    let%lwt res, body = Util.http_get uri ~headers in
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

let fetch_blob ~pds_endpoint ~access_jwt ~did ~cid =
  let uri =
    Uri.with_path (Uri.of_string pds_endpoint) "/xrpc/com.atproto.sync.getBlob"
    |> fun u -> Uri.add_query_params' u [("did", did); ("cid", cid)]
  in
  let headers =
    Http.Header.of_list [("Authorization", "Bearer " ^ access_jwt)]
  in
  try%lwt
    let%lwt res, body = Util.http_get uri ~headers in
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
    let%lwt res, body = Util.http_get uri ~headers in
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
                log "migration: failed to parse preferences response: %s" e ) ;
            Lwt.return_ok (`List []) )
    | status ->
        let%lwt () = Body.drain_body body in
        Dream.warning (fun log ->
            log "migration: failed to fetch preferences: %s"
              (Http.Status.to_string status) ) ;
        Lwt.return_ok (`List [])
  with exn ->
    Dream.warning (fun log ->
        log "migration: exception fetching preferences: %s"
          (Printexc.to_string exn) ) ;
    Lwt.return_ok (`List [])

(* create account on this pds with existing did *)
let create_migrated_account ~email ~handle ~password ~did ~service_auth_token
    ?invite_code db =
  let open Lwt.Infix in
  (* ensure service auth token is signed by the did we're migrating *)
  let%lwt verified =
    match%lwt
      Jwt.verify_service_jwt ~nsid:"com.atproto.server.createAccount"
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
    (* check if did already exists *)
    match%lwt
      Data_store.get_actor_by_identifier did db
    with
    | Some existing when existing.deactivated_at <> None ->
        (* account exists but is deactivated, resumable migration *)
        Lwt.return_error
          "RESUMABLE: An incomplete migration exists for this account."
    | Some _ ->
        Lwt.return_error
          "An account with this DID already exists on this PDS. If you \
           previously migrated here, try logging in instead."
    | None -> (
      (* check if handle is available (may need different handle) *)
      match%lwt
        Data_store.get_actor_by_identifier handle db
      with
      | Some _ ->
          Lwt.return_error
            ( "The handle @" ^ handle
            ^ " is already taken on this PDS. You may need to use a different \
               handle." )
      | None -> (
        (* check if email is available *)
        match%lwt
          Data_store.get_actor_by_identifier email db
        with
        | Some _ ->
            Lwt.return_error "An account with this email already exists"
        | None -> (
            (* validate invite code if required *)
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
                (* generate new signing key *)
                let signing_key, signing_pubkey =
                  Kleidos.K256.generate_keypair ()
                in
                let sk_priv_mk = Kleidos.K256.privkey_to_multikey signing_key in
                (* create deactivated actor *)
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

let bytes_to_car_stream (data : bytes) : Car.stream =
 fun () -> Lwt.return (Lwt_seq.Cons (data, fun () -> Lwt.return Lwt_seq.Nil))

let import_repo ~did ~car_data =
  try%lwt
    let%lwt repo = Repository.load ~create:true did in
    let stream = bytes_to_car_stream car_data in
    match%lwt Repository.import_car repo stream with
    | Ok _ ->
        Lwt.return_ok ()
    | Error e ->
        Lwt.return_error ("Failed to import repository: " ^ Printexc.to_string e)
  with exn ->
    Lwt.return_error ("Failed to import repository: " ^ Printexc.to_string exn)

(* import blobs in batches *)
let import_blobs_batch ~pds_endpoint ~access_jwt ~did ~cids =
  let%lwt user_db = User_store.connect ~create:true did in
  let%lwt results =
    Lwt_list.map_p
      (fun cid_str ->
        match%lwt fetch_blob ~pds_endpoint ~access_jwt ~did ~cid:cid_str with
        | Error e ->
            Dream.warning (fun log ->
                log "migration %s: failed to fetch blob %s: %s" did cid_str e ) ;
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

(* remove trailing slash from pds endpoint *)
let normalize_endpoint s =
  if String.length s > 0 && s.[String.length s - 1] = '/' then
    String.sub s 0 (String.length s - 1)
  else s

(* check if plc identity has been updated to point to this pds *)
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

let check_local_account_status ~did =
  try%lwt
    match%lwt get_account_status did with
    | Ok status ->
        Lwt.return_ok status
    | _ ->
        Lwt.return_error "Failed to load account data"
  with exn ->
    Lwt.return_error
      ("Failed to check account status: " ^ Printexc.to_string exn)

let list_local_missing_blobs ~did ~limit ?cursor () =
  try%lwt
    let%lwt {db= us; _} = Repository.load did in
    let cursor = Option.value ~default:"" cursor in
    let%lwt blobs = User_store.list_missing_blobs ~limit ~cursor us in
    let cids = List.map (fun (_, cid) -> Cid.to_string cid) blobs in
    (* done if we get fewer blobs than limit *)
    let next_cursor =
      if List.length blobs >= limit then List.nth_opt cids (List.length cids - 1)
      else None
    in
    Lwt.return_ok (cids, next_cursor)
  with exn ->
    Lwt.return_error ("Failed to list missing blobs: " ^ Printexc.to_string exn)

let activate_account did db =
  let%lwt () = Data_store.activate_actor did db in
  let%lwt _ =
    Sequencer.sequence_account db ~did ~active:true ~status:`Active ()
  in
  Lwt.return_unit

(* deactivate account on old pds after successful migration *)
let deactivate_old_account ~pds_endpoint ~access_jwt =
  let uri =
    Uri.with_path
      (Uri.of_string pds_endpoint)
      "/xrpc/com.atproto.server.deactivateAccount"
  in
  let headers =
    Http.Header.of_list
      [ ("Authorization", "Bearer " ^ access_jwt)
      ; ("Content-Type", "application/json") ]
  in
  try%lwt
    let%lwt res, body =
      Cohttp_lwt_unix.Client.post ~headers ~body:(Body.of_string "{}") uri
    in
    match res.status with
    | `OK ->
        let%lwt () = Body.drain_body body in
        Lwt.return_ok ()
    | status ->
        let%lwt body_str = Body.to_string body in
        Lwt.return_error
          (Printf.sprintf "Failed to deactivate old account (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

(* possible states for an existing deactivated account for resumption *)
type resume_state =
  | NeedsRepoImport (* account exists but no repo *)
  | NeedsBlobImport (* repo exists, may need blobs *)
  | NeedsPlcUpdate (* data imported, needs plc update *)
  | NeedsActivation (* plc points here, just needs activation *)
  | AlreadyActive (* account is already active *)

let check_resume_state ~did db =
  match%lwt Data_store.get_actor_by_identifier did db with
  | None ->
      Lwt.return_error "Account not found"
  | Some actor when actor.deactivated_at = None ->
      Lwt.return_ok AlreadyActive
  | Some _actor -> (
    (* account is deactivated, check if identity already points here *)
    match%lwt
      check_identity_updated did
    with
    | Ok true ->
        (* just needs activation *)
        Lwt.return_ok NeedsActivation
    | _ -> (
      (* check if repo exists; error probably means it doesn't *)
      try%lwt
        let%lwt us = User_store.connect ~create:false did in
        let%lwt record_count = User_store.count_records us in
        if record_count > 0 then
          (* repo exists, check if we need blob import *)
          match%lwt User_store.count_blobs us with
          | cnt when cnt > 0 ->
              (* data imported, needs plc update *)
              Lwt.return_ok NeedsPlcUpdate
          | _ ->
              (* no blobs, need to start from blob import *)
              Lwt.return_ok NeedsBlobImport
        else
          (* no repo, need to start from repo import *)
          Lwt.return_ok NeedsRepoImport
      with _ -> Lwt.return_ok NeedsRepoImport ) )

let get_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let invite_required = Env.invite_required in
      let hostname = Env.hostname in
      (* check for existing migration state *)
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
            ; identifier= None
            ; invite_code= None
            ; blobs_imported= 0
            ; blobs_failed= 0
            ; old_account_deactivated= false
            ; old_account_deactivation_error= None
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
              ; identifier= None
              ; invite_code= None
              ; blobs_imported= state.blobs_imported
              ; blobs_failed= state.blobs_failed
              ; old_account_deactivated= false
              ; old_account_deactivation_error= None
              ; error= None
              ; message= None }
            else
              { csrf_token
              ; invite_required
              ; hostname
              ; step= "importing_data"
              ; did= Some state.did
              ; handle= Some state.handle
              ; old_pds= Some state.old_pds
              ; identifier= None
              ; invite_code= None
              ; blobs_imported= state.blobs_imported
              ; blobs_failed= state.blobs_failed
              ; old_account_deactivated= false
              ; old_account_deactivation_error= None
              ; error= None
              ; message= None }
      in
      Util.render_html ~title:"Migrate Account"
        (module Frontend.MigratePage)
        ~props )

let post_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let invite_required = Env.invite_required in
      let hostname = Env.hostname in
      let make_props ?(step = "enter_credentials") ?did ?handle ?old_pds
          ?identifier ?invite_code ?(blobs_imported = 0) ?(blobs_failed = 0)
          ?(old_account_deactivated = false) ?old_account_deactivation_error
          ?error ?message () : Frontend.MigratePage.props =
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
      in
      let render_error ?(step = "enter_credentials") ?did ?handle ?old_pds
          ?identifier ?invite_code error =
        Util.render_html ~status:`Bad_Request ~title:"Migrate Account"
          (module Frontend.MigratePage)
          ~props:
            (make_props ~step ?did ?handle ?old_pds ?identifier ?invite_code
               ~error () )
      in
      (* helper to transition to PLC token step after data import *)
      let transition_to_plc_token_step ~did ~handle ~old_pds ~access_jwt
          ~refresh_jwt ~email ~blobs_imported ~blobs_failed =
        (* import preferences before transitioning *)
        let%lwt () =
          match%lwt fetch_preferences ~pds_endpoint:old_pds ~access_jwt with
          | Ok prefs ->
              Data_store.put_preferences ~did ~prefs ctx.db
          | _ ->
              Lwt.return_unit
        in
        (* don't need plc step for did:web *)
        if String.starts_with ~prefix:"did:web:" did then
          (* check if identity already points here *)
          match%lwt check_identity_updated did with
          | Ok true ->
              (* identity already points here, activate directly *)
              let%lwt () = activate_account did ctx.db in
              let%lwt () = Session.log_in_did ctx.req did in
              let%lwt deactivation_result =
                deactivate_old_account ~pds_endpoint:old_pds ~access_jwt
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
                  (make_props ~step:"complete" ~did ~handle ~blobs_imported
                     ~blobs_failed ~old_account_deactivated
                     ?old_account_deactivation_error
                     ~message:
                       "Your account has been successfully migrated! Your \
                        did:web identity is pointing to this PDS."
                     () )
          | _ ->
              (* identity not updated yet - show instructions *)
              Util.render_html ~title:"Migrate Account"
                (module Frontend.MigratePage)
                ~props:
                  (make_props ~step:"error" ~did ~handle ~blobs_imported
                     ~blobs_failed
                     ~error:
                       (Printf.sprintf
                          "Your account uses did:web which requires manual \
                           configuration. Please update your \
                           .well-known/did.json at %s to point to this PDS \
                           (%s), then try resuming the migration."
                          (String.sub did 8 (String.length did - 8))
                          Env.host_endpoint )
                     () )
        else
          (* did:plc, regular flow *)
          match%lwt
            request_plc_operation_signature ~pds_endpoint:old_pds ~access_jwt
          with
          | Error e ->
              Dream.warning (fun log ->
                  log "migration %s: failed to request PLC signature: %s" did e ) ;
              (* still show the token step, user may have received email already *)
              let%lwt () =
                set_migration_state ctx.req
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
                  (make_props ~step:"enter_plc_token" ~did ~handle ~old_pds
                     ~blobs_imported ~blobs_failed
                     ~message:
                       "Data import complete! Check your email for a PLC \
                        confirmation code."
                     ~error:
                       ( "Note: Could not automatically request PLC signature: "
                       ^ e
                       ^ ". You may need to request it manually from your old \
                          PDS." )
                     () )
          | Ok () ->
              let%lwt () =
                set_migration_state ctx.req
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
                  (make_props ~step:"enter_plc_token" ~did ~handle ~old_pds
                     ~blobs_imported ~blobs_failed
                     ~message:
                       "Data import complete! Check your email for a PLC \
                        confirmation code."
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
              (* for jumping to specific steps while debugging *)
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
              | Some "resume_available" ->
                  Util.render_html ~title:"Migrate Account"
                    (module Frontend.MigratePage)
                    ~props:
                      (make_props ~step:"resume_available" ~did:"did:plc:a1b2c3"
                         ~handle:"test.user" ~old_pds:"https://bsky.social" () )
              | Some "importing_data" ->
                  let%lwt () =
                    set_migration_state ctx.req
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
                      (make_props ~step:"importing_data" ~did:"did:plc:a1b2c3"
                         ~handle:"test.user" ~old_pds:"https://bsky.social"
                         ~blobs_imported:42 ~blobs_failed:3 () )
              | Some "enter_plc_token" ->
                  let%lwt () =
                    set_migration_state ctx.req
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
                      (make_props ~step:"enter_plc_token"
                         ~did:"did:plc:testuser123" ~handle:"test.user"
                         ~old_pds:"https://bsky.social" ~blobs_imported:100
                         ~blobs_failed:0
                         ~message:
                           "Data import complete! Check your email for a PLC \
                            confirmation code."
                         () )
              | Some "complete" ->
                  Util.render_html ~title:"Migrate Account"
                    (module Frontend.MigratePage)
                    ~props:
                      (make_props ~step:"complete" ~did:"did:plc:testuser123"
                         ~handle:"test.user" ~blobs_imported:100 ~blobs_failed:0
                         ~old_account_deactivated:true
                         ~message:"Your account has been successfully migrated!"
                         () )
              | Some "complete_deactivation_failed" ->
                  Util.render_html ~title:"Migrate Account"
                    (module Frontend.MigratePage)
                    ~props:
                      (make_props ~step:"complete" ~did:"did:plc:testuser123"
                         ~handle:"test.user" ~blobs_imported:95 ~blobs_failed:5
                         ~old_account_deactivated:false
                         ~old_account_deactivation_error:
                           "Failed to deactivate old account (401): \
                            Unauthorized"
                         ~message:"Your account has been successfully migrated!"
                         () )
              | Some "error" ->
                  Util.render_html ~title:"Migrate Account"
                    (module Frontend.MigratePage)
                    ~props:(make_props ~step:"error" ())
              | _ -> (
                  if
                    (* normal flow *)
                    String.length identifier = 0
                  then render_error "Please enter your handle or DID"
                  else if String.length password = 0 then
                    render_error "Please enter your password"
                  else
                    (* step 1: resolve identity *)
                    match%lwt resolve_identity identifier with
                    | Error e ->
                        render_error e
                    | Ok (did, handle, old_pds) -> (
                      (* step 2: authenticate with old pds *)
                      match%lwt
                        create_session_on_pds ~pds_endpoint:old_pds ~identifier
                          ~password ()
                      with
                      | AuthError e ->
                          render_error e
                      | AuthNeeds2FA ->
                          (* show 2FA form *)
                          Util.render_html ~title:"Migrate Account"
                            (module Frontend.MigratePage)
                            ~props:
                              (make_props ~step:"enter_2fa" ~identifier ~old_pds
                                 ?invite_code () )
                      | AuthSuccess session -> (
                        (* step 3: get session info for account status and email *)
                        match%lwt
                          get_session ~pds_endpoint:old_pds
                            ~access_jwt:session.access_jwt
                        with
                        | Error e ->
                            render_error ("Failed to get account info: " ^ e)
                        | Ok session_info -> (
                            let is_active =
                              match session_info.active with
                              | Some false ->
                                  false
                              | _ ->
                                  true (* default to true if not specified *)
                            in
                            if not is_active then
                              render_error
                                "This account is already deactivated. Cannot \
                                 migrate a deactivated account."
                            else
                              (* step 4: get service auth token *)
                              match%lwt
                                get_service_auth ~pds_endpoint:old_pds
                                  ~access_jwt:session.access_jwt
                              with
                              | Error e ->
                                  render_error
                                    ("Failed to get service authorization: " ^ e)
                              | Ok service_auth_token -> (
                                  (* use real email from old PDS, fallback to placeholder *)
                                  let email =
                                    match session_info.email with
                                    | Some e when String.length e > 0 ->
                                        e
                                    | _ ->
                                        Printf.sprintf "%s@%s" did Env.hostname
                                  in
                                  (* step 5: create account *)
                                  match%lwt
                                    create_migrated_account ~email ~handle
                                      ~password ~did ~service_auth_token
                                      ?invite_code ctx.db
                                  with
                                  | Error e
                                    when String.starts_with ~prefix:"RESUMABLE:"
                                           e -> (
                                    (* try to automatically resume *)
                                    match%lwt
                                      check_resume_state ~did ctx.db
                                    with
                                    | Error e ->
                                        render_error ~did ~handle ~old_pds e
                                    | Ok AlreadyActive ->
                                        (* account is already active, just log them in *)
                                        let%lwt () =
                                          Session.log_in_did ctx.req did
                                        in
                                        Util.render_html
                                          ~title:"Migrate Account"
                                          (module Frontend.MigratePage)
                                          ~props:
                                            (make_props ~step:"complete" ~did
                                               ~handle
                                               ~message:
                                                 "Your account is already \
                                                  active! You have been logged \
                                                  in."
                                               () )
                                    | Ok NeedsActivation ->
                                        (* identity already points here, just activate *)
                                        let%lwt () =
                                          activate_account did ctx.db
                                        in
                                        let%lwt () =
                                          Session.log_in_did ctx.req did
                                        in
                                        let%lwt deactivation_result =
                                          match%lwt
                                            deactivate_old_account
                                              ~pds_endpoint:old_pds
                                              ~access_jwt:session.access_jwt
                                          with
                                          | Ok () ->
                                              Lwt.return_ok ()
                                          | Error err
                                            when Util.str_contains ~affix:"401"
                                                   err
                                                 || Util.str_contains
                                                      ~affix:"Unauthorized" err
                                            -> (
                                            match%lwt
                                              refresh_session
                                                ~pds_endpoint:old_pds
                                                ~refresh_jwt:session.refresh_jwt
                                            with
                                            | Ok tokens ->
                                                deactivate_old_account
                                                  ~pds_endpoint:old_pds
                                                  ~access_jwt:tokens.access_jwt
                                            | Error refresh_err ->
                                                Lwt.return_error
                                                  (Printf.sprintf
                                                     "Token expired and \
                                                      refresh failed: %s"
                                                     refresh_err ) )
                                          | Error err ->
                                              Lwt.return_error err
                                        in
                                        let ( old_account_deactivated
                                            , old_account_deactivation_error ) =
                                          match deactivation_result with
                                          | Ok () ->
                                              (true, None)
                                          | Error err ->
                                              Dream.warning (fun log ->
                                                  log
                                                    "migration %s: failed to \
                                                     deactivate old account: \
                                                     %s"
                                                    did err ) ;
                                              (false, Some err)
                                        in
                                        Util.render_html
                                          ~title:"Migrate Account"
                                          (module Frontend.MigratePage)
                                          ~props:
                                            (make_props ~step:"complete" ~did
                                               ~handle ~old_account_deactivated
                                               ?old_account_deactivation_error
                                               ~message:
                                                 "Your account has been \
                                                  activated! Your identity is \
                                                  pointing to this PDS."
                                               () )
                                    | Ok NeedsPlcUpdate ->
                                        (* data is imported, need plc update *)
                                        transition_to_plc_token_step ~did
                                          ~handle ~old_pds
                                          ~access_jwt:session.access_jwt
                                          ~refresh_jwt:session.refresh_jwt
                                          ~email ~blobs_imported:0
                                          ~blobs_failed:0
                                    | Ok NeedsRepoImport | Ok NeedsBlobImport
                                      -> (
                                      (* need to re-import data, continue with normal flow *)
                                      match%lwt
                                        fetch_repo ~pds_endpoint:old_pds
                                          ~access_jwt:session.access_jwt ~did
                                      with
                                      | Error err ->
                                          render_error ~did ~handle ~old_pds
                                            ( "Failed to fetch repository: "
                                            ^ err )
                                      | Ok car_data -> (
                                        match%lwt
                                          import_repo ~did ~car_data
                                        with
                                        | Error err ->
                                            render_error ~did ~handle ~old_pds
                                              err
                                        | Ok () ->
                                            (* continue with blob import like normal flow *)
                                            transition_to_plc_token_step ~did
                                              ~handle ~old_pds
                                              ~access_jwt:session.access_jwt
                                              ~refresh_jwt:session.refresh_jwt
                                              ~email ~blobs_imported:0
                                              ~blobs_failed:0 ) ) )
                                  | Error e ->
                                      render_error e
                                  | Ok _signing_key_did -> (
                                    (* step 5: fetch and import repo *)
                                    match%lwt
                                      fetch_repo ~pds_endpoint:old_pds
                                        ~access_jwt:session.access_jwt ~did
                                    with
                                    | Error e ->
                                        render_error
                                          ("Failed to fetch repository: " ^ e)
                                    | Ok car_data -> (
                                      match%lwt import_repo ~did ~car_data with
                                      | Error e ->
                                          render_error e
                                      | Ok () -> (
                                          (* log account status after repo import *)
                                          let%lwt () =
                                            match%lwt
                                              check_local_account_status ~did
                                            with
                                            | Ok status ->
                                                Dream.info (fun log ->
                                                    log
                                                      "migration %s: repo \
                                                       imported, \
                                                       indexed_records=%d, \
                                                       expected_blobs=%d"
                                                      did status.indexed_records
                                                      status.expected_blobs ) ;
                                                Lwt.return_unit
                                            | Error e ->
                                                Dream.warning (fun log ->
                                                    log
                                                      "migration %s: failed to \
                                                       check account status: \
                                                       %s"
                                                      did e ) ;
                                                Lwt.return_unit
                                          in
                                          (* step 6: list missing blobs to import *)
                                          match%lwt
                                            list_local_missing_blobs ~did
                                              ~limit:50 ()
                                          with
                                          | Error e ->
                                              Dream.warning (fun log ->
                                                  log
                                                    "migration %s: failed to \
                                                     list missing blobs: %s"
                                                    did e ) ;
                                              (* skip blobs, go to plc token step *)
                                              transition_to_plc_token_step ~did
                                                ~handle ~old_pds
                                                ~access_jwt:session.access_jwt
                                                ~refresh_jwt:session.refresh_jwt
                                                ~email ~blobs_imported:0
                                                ~blobs_failed:0
                                          | Ok (missing_cids, next_cursor) ->
                                              if List.length missing_cids = 0
                                              then
                                                (* no missing blobs, go to plc token step *)
                                                transition_to_plc_token_step
                                                  ~did ~handle ~old_pds
                                                  ~access_jwt:session.access_jwt
                                                  ~refresh_jwt:
                                                    session.refresh_jwt ~email
                                                  ~blobs_imported:0
                                                  ~blobs_failed:0
                                              else
                                                (* import this batch of missing blobs *)
                                                let%lwt imported, failed =
                                                  import_blobs_batch
                                                    ~pds_endpoint:old_pds
                                                    ~access_jwt:
                                                      session.access_jwt ~did
                                                    ~cids:missing_cids
                                                in
                                                (* store state for continuation *)
                                                let cursor =
                                                  Option.value ~default:""
                                                    next_cursor
                                                in
                                                (* check if there are more missing blobs *)
                                                if String.length cursor = 0 then
                                                  (* no more missing blobs, go to plc *)
                                                  transition_to_plc_token_step
                                                    ~did ~handle ~old_pds
                                                    ~access_jwt:
                                                      session.access_jwt
                                                    ~refresh_jwt:
                                                      session.refresh_jwt ~email
                                                    ~blobs_imported:imported
                                                    ~blobs_failed:failed
                                                else
                                                  (* more blobs to import, save state *)
                                                  let%lwt () =
                                                    set_migration_state ctx.req
                                                      { did
                                                      ; handle
                                                      ; old_pds
                                                      ; access_jwt=
                                                          session.access_jwt
                                                      ; refresh_jwt=
                                                          session.refresh_jwt
                                                      ; email
                                                      ; blobs_imported= imported
                                                      ; blobs_failed= failed
                                                      ; blobs_cursor= cursor
                                                      ; plc_requested= false }
                                                  in
                                                  Util.render_html
                                                    ~title:"Migrate Account"
                                                    (module Frontend.MigratePage)
                                                    ~props:
                                                      (make_props
                                                         ~step:"importing_data"
                                                         ~did ~handle ~old_pds
                                                         ~blobs_imported:
                                                           imported
                                                         ~blobs_failed:failed () )
                                          ) ) ) ) ) ) ) ) )
          | "continue_blobs" -> (
            match get_migration_state ctx.req with
            | None ->
                render_error "Migration state not found. Please start over."
            | Some state -> (
                (* refresh token if needed before continuing blob import *)
                let%lwt state =
                  if jwt_needs_refresh state.access_jwt then (
                    match%lwt
                      refresh_session ~pds_endpoint:state.old_pds
                        ~refresh_jwt:state.refresh_jwt
                    with
                    | Ok tokens ->
                        let new_state =
                          { state with
                            access_jwt= tokens.access_jwt
                          ; refresh_jwt= tokens.refresh_jwt }
                        in
                        let%lwt () = set_migration_state ctx.req new_state in
                        Lwt.return new_state
                    | Error e ->
                        Dream.warning (fun log ->
                            log
                              "migration %s: token refresh failed, continuing \
                               with old token: %s"
                              state.did e ) ;
                        Lwt.return state )
                  else Lwt.return state
                in
                (* continue importing missing blobs *)
                let cursor =
                  if String.length state.blobs_cursor > 0 then
                    Some state.blobs_cursor
                  else None
                in
                match%lwt
                  list_local_missing_blobs ~did:state.did ~limit:50 ?cursor ()
                with
                | Error e ->
                    Dream.warning (fun log ->
                        log "migration %s: failed to list missing blobs: %s"
                          state.did e ) ;
                    (* no more blobs, go to plc token step *)
                    transition_to_plc_token_step ~did:state.did
                      ~handle:state.handle ~old_pds:state.old_pds
                      ~access_jwt:state.access_jwt
                      ~refresh_jwt:state.refresh_jwt ~email:state.email
                      ~blobs_imported:state.blobs_imported
                      ~blobs_failed:state.blobs_failed
                | Ok (missing_cids, next_cursor) ->
                    if List.length missing_cids = 0 then
                      (* no more blobs, go to plc token step *)
                      transition_to_plc_token_step ~did:state.did
                        ~handle:state.handle ~old_pds:state.old_pds
                        ~access_jwt:state.access_jwt
                        ~refresh_jwt:state.refresh_jwt ~email:state.email
                        ~blobs_imported:state.blobs_imported
                        ~blobs_failed:state.blobs_failed
                    else
                      let%lwt imported, failed =
                        import_blobs_batch ~pds_endpoint:state.old_pds
                          ~access_jwt:state.access_jwt ~did:state.did
                          ~cids:missing_cids
                      in
                      let new_imported = state.blobs_imported + imported in
                      let new_failed = state.blobs_failed + failed in
                      let new_cursor = Option.value ~default:"" next_cursor in
                      (* check if there are more missing blobs *)
                      if String.length new_cursor = 0 then
                        (* all done, go to plc *)
                        transition_to_plc_token_step ~did:state.did
                          ~handle:state.handle ~old_pds:state.old_pds
                          ~access_jwt:state.access_jwt
                          ~refresh_jwt:state.refresh_jwt ~email:state.email
                          ~blobs_imported:new_imported ~blobs_failed:new_failed
                      else
                        (* more blobs to import, save state *)
                        let%lwt () =
                          set_migration_state ctx.req
                            { state with
                              blobs_imported= new_imported
                            ; blobs_failed= new_failed
                            ; blobs_cursor= new_cursor }
                        in
                        Util.render_html ~title:"Migrate Account"
                          (module Frontend.MigratePage)
                          ~props:
                            (make_props ~step:"importing_data" ~did:state.did
                               ~handle:state.handle ~old_pds:state.old_pds
                               ~blobs_imported:new_imported
                               ~blobs_failed:new_failed () ) ) )
          | "submit_plc_token" -> (
            match get_migration_state ctx.req with
            | None ->
                render_error "Migration state not found. Please start over."
            | Some state -> (
                let plc_token =
                  List.assoc_opt "plc_token" fields
                  |> Option.value ~default:"" |> String.trim
                in
                if String.length plc_token = 0 then
                  render_error ~step:"enter_plc_token" ~did:state.did
                    ~handle:state.handle ~old_pds:state.old_pds
                    "Please enter the PLC token from your email"
                else
                  (* new rotation keys = current rotation keys - old PDS key(s) + new PDS key *)
                  let%lwt old_pds_keys =
                    match%lwt
                      get_remote_recommended_credentials
                        ~pds_endpoint:state.old_pds ~access_jwt:state.access_jwt
                    with
                    | Ok creds ->
                        Lwt.return creds.rotation_keys
                    | Error e ->
                        Dream.warning (fun log ->
                            log
                              "migration %s: failed to get old PDS \
                               credentials: %s"
                              state.did e ) ;
                        Lwt.return []
                  in
                  let%lwt current_keys =
                    match%lwt get_plc_rotation_keys ~did:state.did with
                    | Ok keys ->
                        Lwt.return keys
                    | Error _ ->
                        Lwt.return []
                  in
                  (* remove old PDS key(s) from current keys *)
                  let keys_to_preserve =
                    List.filter
                      (fun k -> not (List.mem k old_pds_keys))
                      current_keys
                  in
                  (* construct recommended credentials *)
                  match%lwt
                    get_recommended_did_credentials state.did ctx.db
                      ~extra_rotation_keys:keys_to_preserve
                  with
                  | Error e ->
                      render_error ~step:"enter_plc_token" ~did:state.did
                        ~handle:state.handle ~old_pds:state.old_pds
                        ("Failed to get credentials: " ^ e)
                  | Ok credentials -> (
                    (* get old pds to sign plc operation *)
                    match%lwt
                      sign_plc_operation ~pds_endpoint:state.old_pds
                        ~access_jwt:state.access_jwt ~token:plc_token
                        ~credentials
                    with
                    | Error e ->
                        render_error ~step:"enter_plc_token" ~did:state.did
                          ~handle:state.handle ~old_pds:state.old_pds
                          ("Failed to sign PLC operation: " ^ e)
                    | Ok signed_operation -> (
                      (* submit plc operation *)
                      match%lwt
                        submit_plc_operation ~did:state.did ~handle:state.handle
                          ~operation:signed_operation ctx.db
                      with
                      | Error e ->
                          render_error ~step:"enter_plc_token" ~did:state.did
                            ~handle:state.handle ~old_pds:state.old_pds
                            ("Failed to submit PLC operation: " ^ e)
                      | Ok () ->
                          (* log account status before activation *)
                          let%lwt () =
                            match%lwt
                              check_local_account_status ~did:state.did
                            with
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
                                      "migration %s: failed to check status \
                                       before activation: %s"
                                      state.did e ) ;
                                Lwt.return_unit
                          in
                          (* activate the account *)
                          let%lwt () = activate_account state.did ctx.db in
                          let%lwt () = Session.log_in_did ctx.req state.did in
                          let%lwt () = clear_migration_state ctx.req in
                          (* try deactivating old account with current token, refresh if expired *)
                          let%lwt deactivation_result =
                            match%lwt
                              deactivate_old_account ~pds_endpoint:state.old_pds
                                ~access_jwt:state.access_jwt
                            with
                            | Ok () ->
                                Lwt.return_ok ()
                            | Error e
                              when Util.str_contains ~affix:"401" e
                                   || Util.str_contains ~affix:"Unauthorized" e
                              -> (
                              match%lwt
                                refresh_session ~pds_endpoint:state.old_pds
                                  ~refresh_jwt:state.refresh_jwt
                              with
                              | Ok tokens ->
                                  deactivate_old_account
                                    ~pds_endpoint:state.old_pds
                                    ~access_jwt:tokens.access_jwt
                              | Error refresh_err ->
                                  Lwt.return_error
                                    (Printf.sprintf
                                       "Token expired and refresh failed: %s"
                                       refresh_err ) )
                            | Error e ->
                                Lwt.return_error e
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
                              (make_props ~step:"complete" ~did:state.did
                                 ~handle:state.handle
                                 ~blobs_imported:state.blobs_imported
                                 ~blobs_failed:state.blobs_failed
                                 ~old_account_deactivated
                                 ?old_account_deactivation_error
                                 ~message:
                                   "Your account has been successfully \
                                    migrated!"
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
                      (make_props ~step:"enter_plc_token" ~did:state.did
                         ~handle:state.handle ~old_pds:state.old_pds
                         ~error:("Failed to resend: " ^ e) () )
              | Ok () ->
                  Util.render_html ~title:"Migrate Account"
                    (module Frontend.MigratePage)
                    ~props:
                      (make_props ~step:"enter_plc_token" ~did:state.did
                         ~handle:state.handle ~old_pds:state.old_pds
                         ~message:"Confirmation code resent! Check your email."
                         () ) ) )
          | "submit_2fa" -> (
              let identifier =
                List.assoc_opt "identifier" fields
                |> Option.value ~default:"" |> String.trim
              in
              let old_pds =
                List.assoc_opt "old_pds" fields
                |> Option.value ~default:"" |> String.trim
              in
              let auth_factor_token =
                List.assoc_opt "auth_factor_token" fields
                |> Option.value ~default:"" |> String.trim
              in
              let invite_code =
                List.assoc_opt "invite_code" fields |> Option.map String.trim
              in
              let password =
                List.assoc_opt "password" fields |> Option.value ~default:""
              in
              if String.length auth_factor_token = 0 then
                render_error ~step:"enter_2fa" ~identifier ~old_pds ?invite_code
                  "Please enter your authentication code"
              else
                (* re-authenticate with 2fa token *)
                match%lwt resolve_identity identifier with
                | Error e ->
                    render_error ~step:"enter_2fa" ~identifier ~old_pds
                      ?invite_code e
                | Ok (did, handle, resolved_pds) -> (
                    let pds_endpoint =
                      if String.length old_pds > 0 then old_pds
                      else resolved_pds
                    in
                    match%lwt
                      create_session_on_pds ~pds_endpoint ~identifier ~password
                        ~auth_factor_token ()
                    with
                    | AuthError e ->
                        render_error ~step:"enter_2fa" ~identifier
                          ~old_pds:pds_endpoint ?invite_code e
                    | AuthNeeds2FA ->
                        render_error ~step:"enter_2fa" ~identifier
                          ~old_pds:pds_endpoint ?invite_code
                          "Invalid authentication code. Please try again."
                    | AuthSuccess session -> (
                      (* continue with normal migration flow *)
                      match%lwt
                        get_session ~pds_endpoint ~access_jwt:session.access_jwt
                      with
                      | Error e ->
                          render_error ("Failed to get account info: " ^ e)
                      | Ok session_info -> (
                          let is_active =
                            match session_info.active with
                            | Some false ->
                                false
                            | _ ->
                                true
                          in
                          if not is_active then
                            render_error
                              "This account is already deactivated. Cannot \
                               migrate a deactivated account."
                          else
                            match%lwt
                              get_service_auth ~pds_endpoint
                                ~access_jwt:session.access_jwt
                            with
                            | Error e ->
                                render_error
                                  ("Failed to get service authorization: " ^ e)
                            | Ok service_auth_token -> (
                                let email =
                                  match session_info.email with
                                  | Some e when String.length e > 0 ->
                                      e
                                  | _ ->
                                      Printf.sprintf "%s@%s" did Env.hostname
                                in
                                match%lwt
                                  create_migrated_account ~email ~handle
                                    ~password ~did ~service_auth_token
                                    ?invite_code ctx.db
                                with
                                | Error e ->
                                    render_error e
                                | Ok _signing_key_did -> (
                                  match%lwt
                                    fetch_repo ~pds_endpoint
                                      ~access_jwt:session.access_jwt ~did
                                  with
                                  | Error e ->
                                      render_error
                                        ("Failed to fetch repository: " ^ e)
                                  | Ok car_data -> (
                                    match%lwt import_repo ~did ~car_data with
                                    | Error e ->
                                        render_error e
                                    | Ok () ->
                                        transition_to_plc_token_step ~did
                                          ~handle ~old_pds:pds_endpoint
                                          ~access_jwt:session.access_jwt
                                          ~refresh_jwt:session.refresh_jwt
                                          ~email ~blobs_imported:0
                                          ~blobs_failed:0 ) ) ) ) ) ) )
          | "resume_migration" -> (
              (* resume a previously started migration *)
              let identifier =
                List.assoc_opt "identifier" fields
                |> Option.value ~default:"" |> String.trim
              in
              let password =
                List.assoc_opt "password" fields |> Option.value ~default:""
              in
              if String.length identifier = 0 then
                render_error ~step:"resume_available"
                  "Please enter your handle or DID"
              else if String.length password = 0 then
                render_error ~step:"resume_available"
                  "Please enter your password"
              else
                match%lwt resolve_identity identifier with
                | Error e ->
                    render_error ~step:"resume_available" e
                | Ok (did, handle, old_pds) -> (
                  match%lwt
                    create_session_on_pds ~pds_endpoint:old_pds ~identifier
                      ~password ()
                  with
                  | AuthError e ->
                      render_error ~step:"resume_available" e
                  | AuthNeeds2FA ->
                      (* show 2fa form, for resume we go back to credentials *)
                      Util.render_html ~title:"Migrate Account"
                        (module Frontend.MigratePage)
                        ~props:
                          (make_props ~step:"enter_2fa" ~identifier ~old_pds ())
                  | AuthSuccess session -> (
                      (* get session info for email *)
                      let%lwt email =
                        match%lwt
                          get_session ~pds_endpoint:old_pds
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
                      (* check what state the existing account is in *)
                      match%lwt check_resume_state ~did ctx.db with
                      | Error e ->
                          render_error ~step:"resume_available" ~did ~handle
                            ~old_pds e
                      | Ok AlreadyActive ->
                          (* already active, just log in *)
                          let%lwt () = Session.log_in_did ctx.req did in
                          Util.render_html ~title:"Migrate Account"
                            (module Frontend.MigratePage)
                            ~props:
                              (make_props ~step:"complete" ~did ~handle
                                 ~message:
                                   "Your account is already active! You have \
                                    been logged in."
                                 () )
                      | Ok NeedsActivation ->
                          (* identity already points here, just activate *)
                          let%lwt () = activate_account did ctx.db in
                          let%lwt () = Session.log_in_did ctx.req did in
                          let%lwt deactivation_result =
                            match%lwt
                              deactivate_old_account ~pds_endpoint:old_pds
                                ~access_jwt:session.access_jwt
                            with
                            | Ok () ->
                                Lwt.return_ok ()
                            | Error e
                              when Util.str_contains ~affix:"401" e
                                   || Util.str_contains ~affix:"Unauthorized" e
                              -> (
                              match%lwt
                                refresh_session ~pds_endpoint:old_pds
                                  ~refresh_jwt:session.refresh_jwt
                              with
                              | Ok tokens ->
                                  deactivate_old_account ~pds_endpoint:old_pds
                                    ~access_jwt:tokens.access_jwt
                              | Error refresh_err ->
                                  Lwt.return_error
                                    (Printf.sprintf
                                       "Token expired and refresh failed: %s"
                                       refresh_err ) )
                            | Error e ->
                                Lwt.return_error e
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
                                      did e ) ;
                                (false, Some e)
                          in
                          Util.render_html ~title:"Migrate Account"
                            (module Frontend.MigratePage)
                            ~props:
                              (make_props ~step:"complete" ~did ~handle
                                 ~old_account_deactivated
                                 ?old_account_deactivation_error
                                 ~message:
                                   "Your account has been activated! Your \
                                    identity is pointing to this PDS."
                                 () )
                      | Ok NeedsPlcUpdate ->
                          (* data is imported, need plc update *)
                          transition_to_plc_token_step ~did ~handle ~old_pds
                            ~access_jwt:session.access_jwt
                            ~refresh_jwt:session.refresh_jwt ~email
                            ~blobs_imported:0 ~blobs_failed:0
                      | Ok NeedsRepoImport | Ok NeedsBlobImport -> (
                        (* need to re-import data *)
                        match%lwt
                          fetch_repo ~pds_endpoint:old_pds
                            ~access_jwt:session.access_jwt ~did
                        with
                        | Error e ->
                            render_error ~did ~handle ~old_pds
                              ("Failed to fetch repository: " ^ e)
                        | Ok car_data -> (
                          match%lwt import_repo ~did ~car_data with
                          | Error e ->
                              render_error ~did ~handle ~old_pds e
                          | Ok () -> (
                            (* list missing blobs locally *)
                            match%lwt
                              list_local_missing_blobs ~did ~limit:50 ()
                            with
                            | Error e ->
                                Dream.warning (fun log ->
                                    log
                                      "migration %s: failed to list missing \
                                       blobs: %s"
                                      did e ) ;
                                transition_to_plc_token_step ~did ~handle
                                  ~old_pds ~access_jwt:session.access_jwt
                                  ~refresh_jwt:session.refresh_jwt ~email
                                  ~blobs_imported:0 ~blobs_failed:0
                            | Ok (missing_cids, next_cursor) ->
                                if List.length missing_cids = 0 then
                                  transition_to_plc_token_step ~did ~handle
                                    ~old_pds ~access_jwt:session.access_jwt
                                    ~refresh_jwt:session.refresh_jwt ~email
                                    ~blobs_imported:0 ~blobs_failed:0
                                else
                                  let%lwt imported, failed =
                                    import_blobs_batch ~pds_endpoint:old_pds
                                      ~access_jwt:session.access_jwt ~did
                                      ~cids:missing_cids
                                  in
                                  let cursor =
                                    Option.value ~default:"" next_cursor
                                  in
                                  if String.length cursor = 0 then
                                    transition_to_plc_token_step ~did ~handle
                                      ~old_pds ~access_jwt:session.access_jwt
                                      ~refresh_jwt:session.refresh_jwt ~email
                                      ~blobs_imported:imported
                                      ~blobs_failed:failed
                                  else
                                    let%lwt () =
                                      set_migration_state ctx.req
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
                                        (make_props ~step:"importing_data" ~did
                                           ~handle ~old_pds
                                           ~blobs_imported:imported
                                           ~blobs_failed:failed () ) ) ) ) ) ) )
          | _ ->
              render_error "Invalid action" )
      | _ ->
          render_error "Invalid form submission" )
