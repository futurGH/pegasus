(* generated from com.atproto.temp.checkHandleAvailability *)

type suggestion =
  {
    handle: string;
    method_: string [@key "method"];
  }
[@@deriving yojson {strict= false}]

type result_unavailable =
  {
    suggestions: suggestion list;
  }
[@@deriving yojson {strict= false}]

type result_available = unit
let result_available_of_yojson _ = Ok ()
let result_available_to_yojson () = `Assoc []

(** Checks whether the provided handle is available. If the handle is not available, available suggestions will be returned. Optional inputs will be used to generate suggestions. *)
module Main = struct
  let nsid = "com.atproto.temp.checkHandleAvailability"

  type params =
  {
    handle: string;
    email: string option [@default None];
    birth_date: string option [@key "birthDate"] [@default None];
  }
[@@deriving yojson {strict= false}]

  type result_ =
  | ResultAvailable of result_available
  | ResultUnavailable of result_unavailable
  | Unknown of Yojson.Safe.t

let result__of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "com.atproto.temp.checkHandleAvailability#resultAvailable" ->
        (match result_available_of_yojson json with
         | Ok v -> Ok (ResultAvailable v)
         | Error e -> Error e)
    | "com.atproto.temp.checkHandleAvailability#resultUnavailable" ->
        (match result_unavailable_of_yojson json with
         | Ok v -> Ok (ResultUnavailable v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let result__to_yojson = function
  | ResultAvailable v ->
      (match result_available_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "com.atproto.temp.checkHandleAvailability#resultAvailable") :: fields)
       | other -> other)
  | ResultUnavailable v ->
      (match result_unavailable_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "com.atproto.temp.checkHandleAvailability#resultUnavailable") :: fields)
       | other -> other)
  | Unknown j -> j

type output =
  {
    handle: string;
    result_: result_ [@key "result"];
  }
[@@deriving yojson {strict= false}]

  let call
      ~handle
      ?email
      ?birth_date
      (client : Hermes.client) : output Lwt.t =
    let params : params = {handle; email; birth_date} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

