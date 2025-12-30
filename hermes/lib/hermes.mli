type blob = {type_: string; ref: Cid.t; mime_type: string; size: int64}

exception Xrpc_error of {status: int; error: string; message: string option}

type session =
  { access_jwt: string
  ; refresh_jwt: string
  ; did: string
  ; handle: string
  ; pds_uri: string option
  ; email: string option
  ; email_confirmed: bool option
  ; email_auth_factor: bool option
  ; active: bool option
  ; status: string option }

type client

type credential_manager

val make_client : service:string -> unit -> client

val make_credential_manager : service:string -> unit -> credential_manager

val login :
     credential_manager
  -> identifier:string
  -> password:string
  -> ?auth_factor_token:string
  -> unit
  -> client Lwt.t

val login_client :
     credential_manager
  -> client
  -> identifier:string
  -> password:string
  -> ?auth_factor_token:string
  -> unit
  -> client Lwt.t

val resume : credential_manager -> session:session -> unit -> client Lwt.t

val logout : credential_manager -> unit Lwt.t

val get_manager_session : credential_manager -> session option

val on_session_update : credential_manager -> (session -> unit Lwt.t) -> unit

val on_session_expired : credential_manager -> (unit -> unit Lwt.t) -> unit

val get_session : client -> session option

val get_service : client -> Uri.t

val query :
     client
  -> string
  -> Yojson.Safe.t
  -> (Yojson.Safe.t -> ('a, string) result)
  -> 'a Lwt.t

val procedure :
     client
  -> string
  -> Yojson.Safe.t
  -> Yojson.Safe.t option
  -> (Yojson.Safe.t -> ('a, string) result)
  -> 'a Lwt.t

val procedure_blob :
     client
  -> string
  -> Yojson.Safe.t
  -> bytes
  -> content_type:string
  -> (Yojson.Safe.t -> ('a, string) result)
  -> 'a Lwt.t

val query_bytes : client -> string -> Yojson.Safe.t -> (string * string) Lwt.t

val procedure_bytes :
     client
  -> string
  -> Yojson.Safe.t
  -> string option
  -> content_type:string
  -> (string * string) option Lwt.t

val session_to_yojson : session -> Yojson.Safe.t

val session_of_yojson : Yojson.Safe.t -> (session, string) result

val blob_to_yojson : blob -> Yojson.Safe.t

val blob_of_yojson : Yojson.Safe.t -> (blob, string) result

module Jwt : sig
  type payload =
    { exp: int option
    ; iat: int option
    ; sub: string option
    ; aud: string option
    ; iss: string option }

  val decode_payload : string -> (payload, string) result

  val is_expired : ?buffer_seconds:int -> string -> bool

  val get_expiration : string -> int option
end

module Http_backend : sig
  type response = Cohttp.Response.t * Cohttp_lwt.Body.t

  module type S = sig
    val get : headers:Cohttp.Header.t -> Uri.t -> response Lwt.t

    val post :
         headers:Cohttp.Header.t
      -> body:Cohttp_lwt.Body.t
      -> Uri.t
      -> response Lwt.t
  end

  module Default : S
end

module Client : sig
  type t = client

  module type S = sig
    val make : service:string -> unit -> t

    val make_with_interceptor :
      service:string -> on_request:(t -> unit Lwt.t) -> unit -> t

    val set_session : t -> session -> unit

    val clear_session : t -> unit

    val get_session : t -> session option

    val get_service : t -> Uri.t

    val query :
         t
      -> string
      -> Yojson.Safe.t
      -> (Yojson.Safe.t -> ('a, string) result)
      -> 'a Lwt.t

    val procedure :
         t
      -> string
      -> Yojson.Safe.t
      -> Yojson.Safe.t option
      -> (Yojson.Safe.t -> ('a, string) result)
      -> 'a Lwt.t

    val query_bytes : t -> string -> Yojson.Safe.t -> (string * string) Lwt.t

    val procedure_bytes :
         t
      -> string
      -> Yojson.Safe.t
      -> string option
      -> content_type:string
      -> (string * string) option Lwt.t

    val procedure_blob :
         t
      -> string
      -> Yojson.Safe.t
      -> bytes
      -> content_type:string
      -> (Yojson.Safe.t -> ('a, string) result)
      -> 'a Lwt.t
  end

  module Make (_ : Http_backend.S) : S
end

module Credential_manager : sig
  type t = credential_manager

  module type S = sig
    val make : service:string -> unit -> t

    val on_session_update : t -> (session -> unit Lwt.t) -> unit

    val on_session_expired : t -> (unit -> unit Lwt.t) -> unit

    val get_session : t -> session option

    val login :
         t
      -> identifier:string
      -> password:string
      -> ?auth_factor_token:string
      -> unit
      -> Client.t Lwt.t

    val login_client :
         t
      -> Client.t
      -> identifier:string
      -> password:string
      -> ?auth_factor_token:string
      -> unit
      -> Client.t Lwt.t

    val resume : t -> session:session -> unit -> Client.t Lwt.t

    val logout : t -> unit Lwt.t
  end

  module Make (_ : Client.S) : S
end
