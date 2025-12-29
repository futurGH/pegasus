open Lwt.Syntax

type t =
  { service: Uri.t
  ; mutable session: Types.session option
  ; mutable on_session_update: (Types.session -> unit Lwt.t) option
  ; mutable on_session_expired: (unit -> unit Lwt.t) option
  ; refresh_mutex: Lwt_mutex.t
  ; mutable refresh_promise: unit Lwt.t option }

module type S = sig
  val make : service:string -> unit -> t

  val on_session_update : t -> (Types.session -> unit Lwt.t) -> unit

  val on_session_expired : t -> (unit -> unit Lwt.t) -> unit

  val get_session : t -> Types.session option

  val login :
       t
    -> identifier:string
    -> password:string
    -> ?auth_factor_token:string
    -> unit
    -> Client.t Lwt.t

  val resume : t -> session:Types.session -> unit -> Client.t Lwt.t

  val logout : t -> unit Lwt.t
end

module Make (C : Client.S) : S = struct
  let make ~service () =
    { service= Uri.of_string service
    ; session= None
    ; on_session_update= None
    ; on_session_expired= None
    ; refresh_mutex= Lwt_mutex.create ()
    ; refresh_promise= None }

  let on_session_update t callback = t.on_session_update <- Some callback

  let on_session_expired t callback = t.on_session_expired <- Some callback

  let get_session t = t.session

  (* update session and notify *)
  let update_session t session =
    t.session <- Some session ;
    match t.on_session_update with
    | Some callback ->
        callback session
    | None ->
        Lwt.return_unit

  (* clear session and notify *)
  let clear_session t =
    t.session <- None ;
    match t.on_session_expired with
    | Some callback ->
        callback ()
    | None ->
        Lwt.return_unit

  (* create raw client for auth operations *)
  let make_raw_client t = C.make ~service:(Uri.to_string t.service) ()

  let rec login t ~identifier ~password ?auth_factor_token () =
    let client = make_raw_client t in
    let input =
      Types.login_request_to_yojson
        {Types.identifier; password; auth_factor_token}
    in
    let* session =
      C.procedure client "com.atproto.server.createSession" (`Assoc [])
        (Some input) Types.session_of_yojson
    in
    let* () = update_session t session in
    (* create client with request interceptor for auto-refresh *)
    let authed_client =
      C.make_with_interceptor ~service:(Uri.to_string t.service)
        ~on_request:(fun c -> check_and_refresh t c)
        ()
    in
    C.set_session authed_client session ;
    Lwt.return authed_client

  and resume t ~session () =
    let* () = update_session t session in
    let authed_client =
      C.make_with_interceptor ~service:(Uri.to_string t.service)
        ~on_request:(fun c -> check_and_refresh t c)
        ()
    in
    C.set_session authed_client session ;
    Lwt.return authed_client

  (* refresh the session *)
  and refresh_session t =
    match t.session with
    | None ->
        Types.raise_xrpc_error_raw ~status:401 ~error:"AuthRequired"
          ~message:"No session to refresh" ()
    | Some session ->
        let client = make_raw_client t in
        (* use refresh token for auth *)
        C.set_session client {session with access_jwt= session.refresh_jwt} ;
        Lwt.catch
          (fun () ->
            let* new_session =
              C.procedure client "com.atproto.server.refreshSession" (`Assoc [])
                None Types.session_of_yojson
            in
            let* () = update_session t new_session in
            Lwt.return (Some new_session) )
          (fun exn ->
            match exn with
            | Types.Xrpc_error {error= "ExpiredToken"; _}
            | Types.Xrpc_error {error= "InvalidToken"; _} ->
                let* () = clear_session t in
                Lwt.return None
            | _ ->
                Lwt.reraise exn )

  (* check token expiry and refresh if needed *)
  and check_and_refresh t client =
    match t.session with
    | None ->
        Lwt.return_unit
    | Some session ->
        if Jwt.is_expired ~buffer_seconds:300 session.access_jwt then
          (* token expired or about to expire, need to refresh *)
          Lwt_mutex.with_lock t.refresh_mutex (fun () ->
              (* check again in case another request already refreshed *)
              match t.session with
              | None ->
                  Lwt.return_unit
              | Some current_session ->
                  if
                    Jwt.is_expired ~buffer_seconds:300
                      current_session.access_jwt
                  then (
                    let* new_session = refresh_session t in
                    match new_session with
                    | Some s ->
                        C.set_session client s ; Lwt.return_unit
                    | None ->
                        C.clear_session client ;
                        Types.raise_xrpc_error_raw ~status:401
                          ~error:"SessionExpired"
                          ~message:"Failed to refresh session" () )
                  else (
                    (* another request already refreshed, just update our client *)
                    C.set_session client current_session ;
                    Lwt.return_unit ) )
        else Lwt.return_unit

  let logout t =
    match t.session with
    | None ->
        Lwt.return_unit
    | Some session ->
        let client = make_raw_client t in
        C.set_session client session ;
        Lwt.catch
          (fun () ->
            let* (_ : Yojson.Safe.t) =
              C.procedure client "com.atproto.server.deleteSession" (`Assoc [])
                None (fun j -> Ok j )
            in
            let* () = clear_session t in
            Lwt.return_unit )
          (fun _ ->
            (* even if server fails, clear local session *)
            let* () = clear_session t in
            Lwt.return_unit )
end

include Make (Client)
