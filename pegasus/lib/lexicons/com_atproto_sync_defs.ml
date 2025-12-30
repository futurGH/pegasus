(* generated from com.atproto.sync.defs *)

(** String type with known values *)
type host_status = string
let host_status_of_yojson = function
  | `String s -> Ok s
  | _ -> Error "host_status: expected string"
let host_status_to_yojson s = `String s

