(* remote pds xrpc calls for account migration *)
open Lexicons
open Cohttp_lwt

type auth_result =
  | AuthSuccess of Hermes.client
  | AuthNeeds2FA
  | AuthError of string

let rec is_private_address address =
  let lower = String.lowercase_ascii address in
  if String.starts_with ~prefix:"::ffff:" lower then
    is_private_address (String.sub lower 7 (String.length lower - 7))
  else
    let ipv4_private =
      match String.split_on_char '.' lower |> List.map int_of_string_opt with
      | [Some a; Some b; Some _; Some _] ->
          a = 0 || a = 10 || a = 127 || a >= 224
          || (a = 169 && b = 254)
          || (a = 172 && b >= 16 && b <= 31)
          || (a = 192 && b = 168)
      | _ ->
          false
    in
    ipv4_private || lower = "::1" || lower = "::"
    || String.starts_with ~prefix:"fc" lower
    || String.starts_with ~prefix:"fd" lower
    || String.starts_with ~prefix:"fe8" lower
    || String.starts_with ~prefix:"fe9" lower
    || String.starts_with ~prefix:"fea" lower
    || String.starts_with ~prefix:"feb" lower

let validate_service_endpoint endpoint =
  let uri = Uri.of_string endpoint in
  match (Uri.scheme uri, Uri.host uri, Uri.userinfo uri) with
  | Some "https", Some host, None -> (
      let port = Uri.port uri |> Option.value ~default:443 |> string_of_int in
      try%lwt
        let%lwt addresses =
          Lwt_unix.getaddrinfo host port [Unix.AI_SOCKTYPE Unix.SOCK_STREAM]
        in
        if addresses = [] then Lwt.return_error "PDS host did not resolve"
        else
          let has_private =
            List.exists
              (fun (info : Unix.addr_info) ->
                match info.ai_addr with
                | ADDR_INET (address, _) ->
                    is_private_address (Unix.string_of_inet_addr address)
                | ADDR_UNIX _ ->
                    true )
              addresses
          in
          if has_private then
            Lwt.return_error
              "PDS endpoint resolves to a private network address"
          else Lwt.return_ok (Uri.to_string uri)
      with exn ->
        Lwt.return_error
          ("Could not resolve PDS endpoint: " ^ Printexc.to_string exn) )
  | Some "https", Some _, Some _ ->
      Lwt.return_error "PDS endpoint must not contain embedded credentials"
  | _ ->
      Lwt.return_error "PDS endpoint must be an HTTPS URL with a hostname"

let resolve_identity identifier =
  let%lwt did =
    if String.starts_with ~prefix:"did:" identifier then
      Lwt.return_ok identifier
    else
      match%lwt Id_resolver.Handle.resolve ~skip_cache:true identifier with
      | Ok did ->
          Lwt.return_ok did
      | Error e ->
          Log.info (fun log ->
              log "migration: could not resolve identifier %s: %s" identifier e ) ;
          Lwt.return_error
            "We couldn't resolve that handle. Check it and try again, or enter \
             the account DID instead."
  in
  match did with
  | Error e ->
      Lwt.return_error e
  | Ok did -> (
    match%lwt Id_resolver.Did.resolve ~skip_cache:true did with
    | Error e ->
        Log.info (fun log ->
            log "migration: could not resolve DID document %s: %s" did e ) ;
        Lwt.return_error
          "We couldn't load that account's identity document. Check the DID \
           and try again."
    | Ok doc -> (
      match Id_resolver.Did.Document.get_service doc "#atproto_pds" with
      | None ->
          Lwt.return_error "No PDS service found in DID document"
      | Some pds_endpoint -> (
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
          match%lwt validate_service_endpoint pds_endpoint with
          | Error e ->
              Lwt.return_error ("Invalid PDS service endpoint: " ^ e)
          | Ok pds_endpoint ->
              Lwt.return_ok (did, handle, pds_endpoint) ) ) )

let create_session ~service ~identifier ~password ?auth_factor_token () =
  match%lwt validate_service_endpoint service with
  | Error e ->
      Lwt.return (AuthError ("Invalid PDS endpoint: " ^ e))
  | Ok service -> (
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
    | Hermes.Xrpc_error {status= 400; error= "InvalidRequest"; _} ->
        Lwt.return (AuthError "Invalid credentials")
    | Hermes.Xrpc_error {status; error; _} ->
        Lwt.return
          (AuthError
             (Printf.sprintf "Authentication failed: %d %s" status error) )
    | exn ->
        Lwt.return (AuthError ("Network error: " ^ Printexc.to_string exn)) )

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
    let%lwt body, content_type =
      Hermes.query_stream client "com.atproto.sync.getRepo"
        (`Assoc [("did", `String did)])
    in
    let media_type =
      String.split_on_char ';' content_type
      |> List.hd |> String.trim |> String.lowercase_ascii
    in
    if
      media_type <> "application/vnd.ipld.car"
      && media_type <> "application/octet-stream"
    then
      Lwt.return_error
        ("Failed to fetch repo: unexpected content type " ^ content_type)
    else
      let body_stream = Cohttp_lwt.Body.to_stream body in
      let total = ref 0 in
      let max_repo_bytes = 1024 * 1024 * 1024 in
      let rec stream () =
        match%lwt Lwt_stream.get body_stream with
        | None ->
            Lwt.return Lwt_seq.Nil
        | Some chunk ->
            if String.length chunk > max_repo_bytes - !total then
              Lwt.fail_with "repository export exceeds 1 GiB safety limit"
            else (
              total := !total + String.length chunk ;
              Lwt.return (Lwt_seq.Cons (Bytes.unsafe_of_string chunk, stream)) )
      in
      Lwt.return_ok stream
  with
  | Hermes.Xrpc_error {status; error; _} ->
      Lwt.return_error
        (Printf.sprintf "Failed to fetch repo: %d %s" status error)
  | exn ->
      Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

let fetch_blob ~did ~cid client =
  try%lwt
    let%lwt body, content_type =
      Hermes.query_stream client "com.atproto.sync.getBlob"
        (`Assoc [("did", `String did); ("cid", `String cid)])
    in
    let stream = Cohttp_lwt.Body.to_stream body in
    let max_blob_bytes = 250 * 1024 * 1024 in
    let buffer = Buffer.create 65_536 in
    let%lwt () =
      Lwt_unix.with_timeout 120.0 (fun () ->
          Lwt_stream.iter_s
            (fun chunk ->
              if String.length chunk > max_blob_bytes - Buffer.length buffer
              then Lwt.fail_with "blob exceeds 250 MiB safety limit"
              else (
                Buffer.add_string buffer chunk ;
                Lwt.return_unit ) )
            stream )
    in
    Lwt.return_ok (Buffer.to_bytes buffer, content_type)
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
      Lwt.return_error
        (Printf.sprintf "Failed to fetch preferences: %d %s" status error)
  | exn ->
      Log.warn (fun log ->
          log "migration: exception fetching preferences: %s"
            (Printexc.to_string exn) ) ;
      Lwt.return_error ("Failed to fetch preferences: " ^ Printexc.to_string exn)

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
    let rec attempt remaining =
      try%lwt
        let%lwt res, body =
          Lwt_unix.with_timeout 15.0 (fun () ->
              Cohttp_lwt_unix.Client.get
                ~headers:(Http.Header.of_list [("Accept", "application/json")])
                uri )
        in
        let status = Cohttp.Code.code_of_status res.status in
        if status = 200 then
          let%lwt body_str = Body.to_string body in
          try
            let json = Yojson.Safe.from_string body_str in
            let open Yojson.Safe.Util in
            let rotation_keys =
              json |> member "rotationKeys" |> to_list |> List.map to_string
            in
            if rotation_keys = [] then
              Lwt.return_error "PLC directory returned no rotation keys"
            else Lwt.return_ok rotation_keys
          with exn ->
            Lwt.return_error
              ("Invalid PLC directory response: " ^ Printexc.to_string exn)
        else
          let%lwt () = Body.drain_body body in
          if remaining > 1 && (status = 429 || status >= 500) then
            let%lwt () = Lwt_unix.sleep 0.2 in
            attempt (remaining - 1)
          else
            Lwt.return_error
              (Printf.sprintf "PLC directory request failed with status %d"
                 status )
      with exn ->
        if remaining > 1 then
          let%lwt () = Lwt_unix.sleep 0.2 in
          attempt (remaining - 1)
        else
          Lwt.return_error
            ("PLC directory request failed: " ^ Printexc.to_string exn)
    in
    attempt 3
