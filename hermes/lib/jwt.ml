type payload =
  { exp: int option [@default None]
  ; iat: int option [@default None]
  ; sub: string option [@default None]
  ; aud: string option [@default None]
  ; iss: string option [@default None] }
[@@deriving yojson {strict= false}]

(* decode jwt payload without signature verification *)
let decode_payload (jwt : string) : (payload, string) result =
  try
    match String.split_on_char '.' jwt with
    | [_header; payload_str; _signature] -> (
      match
        Base64.decode ~pad:false ~alphabet:Base64.uri_safe_alphabet payload_str
      with
      | Ok decoded -> (
          let json = Yojson.Safe.from_string decoded in
          match payload_of_yojson json with Ok p -> Ok p | Error e -> Error e )
      | Error (`Msg e) ->
          Error ("invalid base64 in JWT: " ^ e) )
    | _ ->
        Error "invalid JWT format"
  with
  | Yojson.Json_error e ->
      Error ("invalid JSON in JWT payload: " ^ e)
  | e ->
      Error (Printexc.to_string e)

(* check if jwt is expired with buffer in seconds *)
let is_expired ?(buffer_seconds = 60) (jwt : string) : bool =
  match decode_payload jwt with
  | Ok {exp= Some exp; _} ->
      let now = int_of_float (Unix.time ()) in
      exp - buffer_seconds <= now
  | Ok {exp= None; _} ->
      (* no expiration, assume not expired *)
      false
  | Error _ ->
      (* can't decode, assume expired to be safe *)
      true

(* get expiration time from jwt *)
let get_expiration (jwt : string) : int option =
  match decode_payload jwt with Ok {exp; _} -> exp | Error _ -> None
