exception InvalidRequestError of (string * string)

exception InternalServerError of (string * string)

exception AuthError of (string * string)

exception NotFoundError of (string * string)

exception Redirect of string

exception UseDpopNonceError

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

let use_dpop_nonce () = raise UseDpopNonceError

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
  | NotFoundError (error, message) ->
      format_response error message `Not_Found
  | UseDpopNonceError ->
      Dream.json ~status:`Bad_Request
        ~headers:
          [ ("WWW-Authenticate", {|DPoP error="use_dpop_nonce"|})
          ; ("Access-Control-Expose-Headers", "WWW-Authenticate") ]
        {|{ "error": "use_dpop_nonce" }|}
  | _ ->
      format_response "InternalServerError" "Internal server error"
        `Internal_Server_Error

let log_exn ?req exn =
  Dream.error (fun log -> log ?request:req "%s" (Printexc.to_string exn))
