(* remote pds xrpc calls for account migration *)

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

type remote_credentials_response =
  Identity.GetRecommendedDidCredentials.response
[@@deriving yojson {strict= false}]

open Cohttp_lwt

type auth_result =
  | AuthSuccess of create_session_response
  | AuthNeeds2FA
  | AuthError of string

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

let create_session ~pds_endpoint ~identifier ~password ?auth_factor_token () =
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

let jwt_needs_refresh ?(delta_s = 60) access_jwt =
  match Jwt.decode_jwt access_jwt with
  | Error _ ->
      true
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

let get_recommended_credentials ~pds_endpoint ~access_jwt =
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

let request_plc_signature ~pds_endpoint ~access_jwt =
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

let deactivate_account ~pds_endpoint ~access_jwt =
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
          (Printf.sprintf "Failed to deactivate account (%s): %s"
             (Http.Status.to_string status)
             body_str )
  with exn -> Lwt.return_error ("Network error: " ^ Printexc.to_string exn)

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
