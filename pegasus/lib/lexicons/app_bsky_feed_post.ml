(* generated from app.bsky.feed.post *)

type reply_ref =
  {
    root: Com_atproto_repo_strongRef.main;
    parent: Com_atproto_repo_strongRef.main;
  }
[@@deriving yojson {strict= false}]

type text_slice =
  {
    start: int;
    end_: int [@key "end"];
  }
[@@deriving yojson {strict= false}]

type entity =
  {
    index: text_slice;
    type_: string [@key "type"];
    value: string;
  }
[@@deriving yojson {strict= false}]

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

type embed =
  | Images of App_bsky_embed_images.main
  | Video of App_bsky_embed_video.main
  | External of App_bsky_embed_external.main
  | Record of App_bsky_embed_record.main
  | RecordWithMedia of App_bsky_embed_recordWithMedia.main
  | Unknown of Yojson.Safe.t

let embed_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.embed.images" ->
        (match App_bsky_embed_images.main_of_yojson json with
         | Ok v -> Ok (Images v)
         | Error e -> Error e)
    | "app.bsky.embed.video" ->
        (match App_bsky_embed_video.main_of_yojson json with
         | Ok v -> Ok (Video v)
         | Error e -> Error e)
    | "app.bsky.embed.external" ->
        (match App_bsky_embed_external.main_of_yojson json with
         | Ok v -> Ok (External v)
         | Error e -> Error e)
    | "app.bsky.embed.record" ->
        (match App_bsky_embed_record.main_of_yojson json with
         | Ok v -> Ok (Record v)
         | Error e -> Error e)
    | "app.bsky.embed.recordWithMedia" ->
        (match App_bsky_embed_recordWithMedia.main_of_yojson json with
         | Ok v -> Ok (RecordWithMedia v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let embed_to_yojson = function
  | Images v ->
      (match App_bsky_embed_images.main_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.images") :: fields)
       | other -> other)
  | Video v ->
      (match App_bsky_embed_video.main_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.video") :: fields)
       | other -> other)
  | External v ->
      (match App_bsky_embed_external.main_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.external") :: fields)
       | other -> other)
  | Record v ->
      (match App_bsky_embed_record.main_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.record") :: fields)
       | other -> other)
  | RecordWithMedia v ->
      (match App_bsky_embed_recordWithMedia.main_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.recordWithMedia") :: fields)
       | other -> other)
  | Unknown j -> j

type main =
  {
    text: string;
    entities: entity list option [@default None];
    facets: App_bsky_richtext_facet.main list option [@default None];
    reply: reply_ref option [@default None];
    embed: embed option [@default None];
    langs: string list option [@default None];
    labels: labels option [@default None];
    tags: string list option [@default None];
    created_at: string [@key "createdAt"];
  }
[@@deriving yojson {strict= false}]

