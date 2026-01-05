exception InvalidRequestError of (string * string)

exception InternalServerError of (string * string)

exception AuthError of (string * string)

exception NotFoundError of (string * string)

exception Redirect of string

(* HTTP 400, { error: "..." } — https://datatracker.ietf.org/doc/html/rfc9449#section-8 *)
exception DpopAuthError of string

(* HTTP 401, WWW-Authenticate=DPoP error="..." — https://datatracker.ietf.org/doc/html/rfc9449#section-9 *)
exception DpopResourceError of string

let is_xrpc_error = function
  | InvalidRequestError _
  | InternalServerError _
  | AuthError _
  | NotFoundError _ ->
      true
  | _ ->
      false

let invalid_request ?(name = "InvalidRequest") msg =
  raise (InvalidRequestError (name, msg))

let internal_error ?(name = "InternalServerError")
    ?(msg = "internal server error") () =
  raise (InternalServerError (name, msg))

let auth_required ?(name = "AuthRequired") msg = raise (AuthError (name, msg))

let not_found ?(name = "NotFound") msg = raise (NotFoundError (name, msg))

let dpop_auth error = raise (DpopAuthError error)

let dpop_resource error = raise (DpopResourceError error)

let printer = function
  | InvalidRequestError (error, message) ->
      Some (Printf.sprintf "Invalid request (%s): %s" error message)
  | InternalServerError (error, message) ->
      Some (Printf.sprintf "Internal server error (%s): %s" error message)
  | AuthError (error, message) ->
      Some (Printf.sprintf "Auth error (%s): %s" error message)
  | NotFoundError (error, message) ->
      Some (Printf.sprintf "Not found (%s): %s" error message)
  | DpopAuthError error ->
      Some (Printf.sprintf "DPoP auth error (%s)" error)
  | DpopResourceError error ->
      Some (Printf.sprintf "DPoP resource error (%s)" error)
  | _ ->
      None

let exn_to_response exn =
  let format_response error msg status =
    Dream.json ~status @@ Yojson.Safe.to_string
    @@ `Assoc [("error", `String error); ("message", `String msg)]
  in
  match exn with
  | InvalidRequestError (error, message) ->
      Log.debug (fun log -> log "invalid request: %s - %s" error message) ;
      format_response error message `Bad_Request
  | InternalServerError (error, message) ->
      Log.debug (fun log -> log "internal server error: %s - %s" error message) ;
      format_response error message `Internal_Server_Error
  | AuthError (error, message) ->
      Log.debug (fun log -> log "auth error: %s - %s" error message) ;
      format_response error message `Unauthorized
  | NotFoundError (error, message) ->
      Log.debug (fun log -> log "not found error: %s - %s" error message) ;
      format_response error message `Not_Found
  | DpopAuthError e ->
      Log.debug (fun log -> log "dpop auth error") ;
      Dream.json ~status:`Bad_Request
        ~headers:[("Access-Control-Expose-Headers", "DPoP-Nonce")]
        (Format.sprintf {|{ "error": "%s" }|} e)
  | DpopResourceError e ->
      Log.debug (fun log -> log "dpop resource error") ;
      Dream.json ~status:`Unauthorized
        ~headers:
          [ ("WWW-Authenticate", Format.sprintf {|DPoP error="%s"|} e)
          ; ("Access-Control-Expose-Headers", "DPoP-Nonce, WWW-Authenticate") ]
        (Format.sprintf {|{ "error": "%s" }|} e)
  | e ->
      Log.warn (fun log ->
          log "unexpected internal error: %s" (Printexc.to_string e) ) ;
      format_response "InternalServerError" "Internal server error"
        `Internal_Server_Error

let log_exn = Log.log_exn
