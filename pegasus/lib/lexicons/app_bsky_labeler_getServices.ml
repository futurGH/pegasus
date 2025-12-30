(* generated from app.bsky.labeler.getServices *)

(** Get information about a list of labeler services. *)
module Main = struct
  let nsid = "app.bsky.labeler.getServices"

  type params =
  {
    dids: string list;
    detailed: bool option [@default None];
  }
[@@deriving yojson {strict= false}]

  type views_item =
  | LabelerView of App_bsky_labeler_defs.labeler_view
  | LabelerViewDetailed of App_bsky_labeler_defs.labeler_view_detailed
  | Unknown of Yojson.Safe.t

let views_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.labeler.defs#labelerView" ->
        (match App_bsky_labeler_defs.labeler_view_of_yojson json with
         | Ok v -> Ok (LabelerView v)
         | Error e -> Error e)
    | "app.bsky.labeler.defs#labelerViewDetailed" ->
        (match App_bsky_labeler_defs.labeler_view_detailed_of_yojson json with
         | Ok v -> Ok (LabelerViewDetailed v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let views_item_to_yojson = function
  | LabelerView v ->
      (match App_bsky_labeler_defs.labeler_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.labeler.defs#labelerView") :: fields)
       | other -> other)
  | LabelerViewDetailed v ->
      (match App_bsky_labeler_defs.labeler_view_detailed_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.labeler.defs#labelerViewDetailed") :: fields)
       | other -> other)
  | Unknown j -> j

type output =
  {
    views: views_item list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~dids
      ?detailed
      (client : Hermes.client) : output Lwt.t =
    let params : params = {dids; detailed} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

