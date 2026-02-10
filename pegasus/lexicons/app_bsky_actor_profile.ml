(* generated from app.bsky.actor.profile *)

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
  { display_name: string option [@key "displayName"] [@default None]
  ; description: string option [@default None]
  ; avatar: Hermes.blob option [@default None]
  ; banner: Hermes.blob option [@default None]
  ; labels: labels option [@default None]
  ; joined_via_starter_pack: Com_atproto_repo_strongRef.main option
        [@key "joinedViaStarterPack"] [@default None]
  ; pinned_post: Com_atproto_repo_strongRef.main option
        [@key "pinnedPost"] [@default None]
  ; created_at: string option [@key "createdAt"] [@default None] }
[@@deriving yojson {strict= false}]
