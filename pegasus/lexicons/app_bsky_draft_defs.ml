(* generated from app.bsky.draft.defs *)

type draft_embed_record =
  {
    record: Com_atproto_repo_strongRef.main;
  }
[@@deriving yojson {strict= false}]

type draft_embed_external =
  {
    uri: string;
  }
[@@deriving yojson {strict= false}]

type draft_embed_caption =
  {
    lang: string;
    content: string;
  }
[@@deriving yojson {strict= false}]

type draft_embed_local_ref =
  {
    path: string;
  }
[@@deriving yojson {strict= false}]

type draft_embed_video =
  {
    local_ref: draft_embed_local_ref [@key "localRef"];
    alt: string option [@default None];
    captions: draft_embed_caption list option [@default None];
  }
[@@deriving yojson {strict= false}]

type draft_embed_image =
  {
    local_ref: draft_embed_local_ref [@key "localRef"];
    alt: string option [@default None];
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

type draft_post =
  {
    text: string;
    labels: labels option [@default None];
    embed_images: draft_embed_image list option [@key "embedImages"] [@default None];
    embed_videos: draft_embed_video list option [@key "embedVideos"] [@default None];
    embed_externals: draft_embed_external list option [@key "embedExternals"] [@default None];
    embed_records: draft_embed_record list option [@key "embedRecords"] [@default None];
  }
[@@deriving yojson {strict= false}]

type threadgate_allow_item =
  | MentionRule of App_bsky_feed_threadgate.mention_rule
  | FollowerRule of App_bsky_feed_threadgate.follower_rule
  | FollowingRule of App_bsky_feed_threadgate.following_rule
  | ListRule of App_bsky_feed_threadgate.list_rule
  | Unknown of Yojson.Safe.t

let threadgate_allow_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.threadgate#mentionRule" ->
        (match App_bsky_feed_threadgate.mention_rule_of_yojson json with
         | Ok v -> Ok (MentionRule v)
         | Error e -> Error e)
    | "app.bsky.feed.threadgate#followerRule" ->
        (match App_bsky_feed_threadgate.follower_rule_of_yojson json with
         | Ok v -> Ok (FollowerRule v)
         | Error e -> Error e)
    | "app.bsky.feed.threadgate#followingRule" ->
        (match App_bsky_feed_threadgate.following_rule_of_yojson json with
         | Ok v -> Ok (FollowingRule v)
         | Error e -> Error e)
    | "app.bsky.feed.threadgate#listRule" ->
        (match App_bsky_feed_threadgate.list_rule_of_yojson json with
         | Ok v -> Ok (ListRule v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let threadgate_allow_item_to_yojson = function
  | MentionRule v ->
      (match App_bsky_feed_threadgate.mention_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.threadgate#mentionRule") :: fields)
       | other -> other)
  | FollowerRule v ->
      (match App_bsky_feed_threadgate.follower_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.threadgate#followerRule") :: fields)
       | other -> other)
  | FollowingRule v ->
      (match App_bsky_feed_threadgate.following_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.threadgate#followingRule") :: fields)
       | other -> other)
  | ListRule v ->
      (match App_bsky_feed_threadgate.list_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.threadgate#listRule") :: fields)
       | other -> other)
  | Unknown j -> j

type postgate_embedding_rules_item =
  | DisableRule of App_bsky_feed_postgate.disable_rule
  | Unknown of Yojson.Safe.t

let postgate_embedding_rules_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.postgate#disableRule" ->
        (match App_bsky_feed_postgate.disable_rule_of_yojson json with
         | Ok v -> Ok (DisableRule v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let postgate_embedding_rules_item_to_yojson = function
  | DisableRule v ->
      (match App_bsky_feed_postgate.disable_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.postgate#disableRule") :: fields)
       | other -> other)
  | Unknown j -> j

type draft =
  {
    device_id: string option [@key "deviceId"] [@default None];
    device_name: string option [@key "deviceName"] [@default None];
    posts: draft_post list;
    langs: string list option [@default None];
    postgate_embedding_rules: postgate_embedding_rules_item list option [@key "postgateEmbeddingRules"] [@default None];
    threadgate_allow: threadgate_allow_item list option [@key "threadgateAllow"] [@default None];
  }
[@@deriving yojson {strict= false}]

type draft_with_id =
  {
    id: string;
    draft: draft;
  }
[@@deriving yojson {strict= false}]

type draft_view =
  {
    id: string;
    draft: draft;
    created_at: string [@key "createdAt"];
    updated_at: string [@key "updatedAt"];
  }
[@@deriving yojson {strict= false}]

