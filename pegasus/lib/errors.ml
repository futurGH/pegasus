exception InvalidRequestError of (string * string)

exception InternalServerError of (string * string)

exception AuthError of (string * string)

let invalid_request ?(name = "InvalidRequest") msg =
  raise (InvalidRequestError (name, msg))

let internal_error ?(name = "InternalServerError")
    ?(msg = "Internal server error") () =
  raise (InternalServerError (name, msg))

let auth_required ?(name = "AuthRequired") msg = raise (AuthError (name, msg))

let exn_to_response exn =
  let format_response error msg status =
    Dream.json ~status @@ Yojson.Safe.to_string
    @@ `Assoc [("error", `String error); ("message", `String msg)]
  in
  match exn with
  | InvalidRequestError (error, message) ->
      format_response error message `Bad_Request
  | InternalServerError (error, message) ->
      format_response error message `Internal_Server_Error
  | AuthError (error, message) ->
      format_response error message `Unauthorized
  | _ ->
      format_response "InternalServerError" "Internal server error"
        `Internal_Server_Error

let log_exn exn = Dream.error (fun log -> log "%s" (Printexc.to_string exn))
