(* generated from app.bsky.labeler.service *)

type labels =
  | SelfLabels of Com_atproto_label_defs.self_labels
  | Unknown of Yojson.Safe.t

let labels_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "com.atproto.label.defs#selfLabels" -> (
      match Com_atproto_label_defs.self_labels_of_yojson json with
      | Ok v ->
          Ok (SelfLabels v)
      | Error e ->
          Error e )
    | _ ->
        Ok (Unknown json)
  with _ -> Error "failed to parse union"

let labels_to_yojson = function
  | SelfLabels v -> (
    match Com_atproto_label_defs.self_labels_to_yojson v with
    | `Assoc fields ->
        `Assoc (("$type", `String "com.atproto.label.defs#selfLabels") :: fields)
    | other ->
        other )
  | Unknown j ->
      j

type main =
  { policies: App_bsky_labeler_defs.labeler_policies
  ; labels: labels option [@default None]
  ; created_at: string [@key "createdAt"]
  ; reason_types: Com_atproto_moderation_defs.reason_type list option
        [@key "reasonTypes"] [@default None]
  ; subject_types: Com_atproto_moderation_defs.subject_type list option
        [@key "subjectTypes"] [@default None]
  ; subject_collections: string list option
        [@key "subjectCollections"] [@default None] }
[@@deriving yojson {strict= false}]
