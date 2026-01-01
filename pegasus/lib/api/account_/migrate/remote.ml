(* remote pds xrpc calls for account migration *)
open Lexicons
open Cohttp_lwt

type auth_result =
  | AuthSuccess of Hermes.client
  | AuthNeeds2FA
  | AuthError of string

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

let create_session ~service ~identifier ~password ?auth_factor_token () =
  try%lwt
    let%lwt client =
      Hermes.login
        (Hermes.make_credential_manager ~service ())
        ~identifier ~password ?auth_factor_token ()
    in
    Lwt.return (AuthSuccess client)
  with
  | Hermes.Xrpc_error {status; error; _}
    when Http.Status.of_int status = `Unauthorized -> (
    match error with
    | "AuthFactorTokenRequired" ->
        Lwt.return AuthNeeds2FA
    | _ ->
        Lwt.return (AuthError "Invalid credentials") )
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return
        (AuthError (Printf.sprintf "Authentication failed: %d %s" status error))
  | exn ->
      Lwt.return (AuthError ("Network error: " ^ Printexc.to_string exn))

let get_session client =
  try%lwt
    let%lwt res = [%xrpc get "com.atproto.server.getSession"] client in
    Lwt.return_ok res
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to get session info: %d %s" status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let jwt_needs_refresh ?(delta_s = 60) access_jwt =
  Hermes.Jwt.is_expired ~buffer_seconds:delta_s access_jwt

let refresh_session client =
  try%lwt
    let%lwt res = [%xrpc post "com.atproto.server.refreshSession"] client in
    Lwt.return_ok res
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to refresh session: %d %s" status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let get_service_auth client =
  try%lwt
    let%lwt res =
      [%xrpc get "com.atproto.server.getServiceAuth"]
        ~aud:Env.did ~lxm:"com.atproto.server.createAccount"
        ~exp:(int_of_float (Unix.gettimeofday ()) + 300)
        client
    in
    Lwt.return_ok res.token
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to get service auth: %d %s" status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let get_recommended_credentials client =
  try%lwt
    let%lwt res =
      [%xrpc get "com.atproto.identity.getRecommendedDidCredentials"] client
    in
    Lwt.return_ok res
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to get recommended credentials: %d %s" status
           error )
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let request_plc_signature client =
  try%lwt
    let%lwt () =
      [%xrpc post "com.atproto.identity.requestPlcOperationSignature"] client
    in
    Lwt.return_ok ()
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to request PLC signature: %d %s" status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let sign_plc_operation ~token ~(credentials : Plc.credentials) client =
  try%lwt
    let verification_methods =
      `Assoc
        (List.map
           (fun (k, v) -> (k, `String v))
           credentials.verification_methods )
    in
    let services = Plc.service_map_to_yojson credentials.services in
    let%lwt res =
      [%xrpc post "com.atproto.identity.signPlcOperation"]
        ~token ~rotation_keys:credentials.rotation_keys ~verification_methods
        ~also_known_as:credentials.also_known_as ~services client
    in
    Lwt.return_ok res.operation
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to sign PLC operation: %d %s" status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let fetch_repo ~did client =
  try%lwt
    let%lwt res = [%xrpc get "com.atproto.sync.getRepo"] ~did client in
    Lwt.return_ok res
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to fetch repo: %d %s" status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let fetch_blob ~did ~cid client =
  try%lwt
    let%lwt res = [%xrpc get "com.atproto.sync.getBlob"] ~did ~cid client in
    Lwt.return_ok res
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to fetch blob %s: %d %s" cid status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let fetch_preferences client =
  try%lwt
    let%lwt res = [%xrpc get "app.bsky.actor.getPreferences"] client in
    Lwt.return_ok res.preferences
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Log.warn (fun log ->
          log "migration: failed to fetch preferences: %d %s" status error ) ;
      Lwt.return_ok []
  | exn ->
      Log.warn (fun log ->
          log "migration: exception fetching preferences: %s"
            (Printexc.to_string exn) ) ;
      Lwt.return_ok []

let deactivate_account client =
  try%lwt
    let%lwt () = [%xrpc post "com.atproto.server.deactivateAccount"] client in
    Lwt.return_ok ()
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to deactivate account: %d %s" status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

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
