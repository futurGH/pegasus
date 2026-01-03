(* generated from app.bsky.feed.generator *)

type labels =
  | SelfLabels of Com_atproto_label_defs.self_labels
  | Unknown of Yojson.Safe.t

let labels_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "com.atproto.label.defs#selfLabels" ->
        (match Com_atproto_label_defs.self_labels_of_yojson json with
         | Ok v -> Ok (SelfLabels v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let labels_to_yojson = function
  | SelfLabels v ->
      (match Com_atproto_label_defs.self_labels_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "com.atproto.label.defs#selfLabels") :: fields)
       | other -> other)
  | Unknown j -> j

type main =
  {
    did: string;
    display_name: string [@key "displayName"];
    description: string option [@default None];
    description_facets: App_bsky_richtext_facet.main list option [@key "descriptionFacets"] [@default None];
    avatar: Hermes.blob option [@default None];
    accepts_interactions: bool option [@key "acceptsInteractions"] [@default None];
    labels: labels option [@default None];
    content_mode: string option [@key "contentMode"] [@default None];
    created_at: string [@key "createdAt"];
  }
[@@deriving yojson {strict= false}]

