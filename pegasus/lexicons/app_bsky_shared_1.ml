(* shared module for lexicons: app.bsky.actor.defs, app.bsky.embed.record, app.bsky.embed.recordWithMedia, app.bsky.feed.defs, app.bsky.graph.defs, app.bsky.labeler.defs *)

type actor_embed =
  | ExternalView of App_bsky_embed_external.view
  | Unknown of Yojson.Safe.t

let actor_embed_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.embed.external#view" ->
        (match App_bsky_embed_external.view_of_yojson json with
         | Ok v -> Ok (ExternalView v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let actor_embed_to_yojson = function
  | ExternalView v ->
      (match App_bsky_embed_external.view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.external#view") :: fields)
       | other -> other)
  | Unknown j -> j

type status_view =
  {
    status: string;
    record: Yojson.Safe.t;
    embed: actor_embed option [@default None];
    expires_at: string option [@key "expiresAt"] [@default None];
    is_active: bool option [@key "isActive"] [@default None];
  }
[@@deriving yojson {strict= false}]

type verification_view =
  {
    issuer: string;
    uri: string;
    is_valid: bool [@key "isValid"];
    created_at: string [@key "createdAt"];
  }
[@@deriving yojson {strict= false}]

type verification_state =
  {
    verifications: verification_view list;
    verified_status: string [@key "verifiedStatus"];
    trusted_verifier_status: string [@key "trustedVerifierStatus"];
  }
[@@deriving yojson {strict= false}]

type list_viewer_state =
  {
    muted: bool option [@default None];
    blocked: string option [@default None];
  }
[@@deriving yojson {strict= false}]

(** string type with known values *)
type list_purpose = string
let list_purpose_of_yojson = function
  | `String s -> Ok s
  | _ -> Error "list_purpose: expected string"
let list_purpose_to_yojson s = `String s

type list_view_basic =
  {
    uri: string;
    cid: string;
    name: string;
    purpose: list_purpose;
    avatar: string option [@default None];
    list_item_count: int option [@key "listItemCount"] [@default None];
    labels: Com_atproto_label_defs.label list option [@default None];
    viewer: list_viewer_state option [@default None];
    indexed_at: string option [@key "indexedAt"] [@default None];
  }
[@@deriving yojson {strict= false}]

type profile_associated_activity_subscription =
  {
    allow_subscriptions: string [@key "allowSubscriptions"];
  }
[@@deriving yojson {strict= false}]

type profile_associated_chat =
  {
    allow_incoming: string [@key "allowIncoming"];
  }
[@@deriving yojson {strict= false}]

type profile_associated =
  {
    lists: int option [@default None];
    feedgens: int option [@default None];
    starter_packs: int option [@key "starterPacks"] [@default None];
    labeler: bool option [@default None];
    chat: profile_associated_chat option [@default None];
    activity_subscription: profile_associated_activity_subscription option [@key "activitySubscription"] [@default None];
  }
[@@deriving yojson {strict= false}]

type profile_view_basic = {
  did: string;
  handle: string;
  display_name: string option [@key "displayName"] [@default None];
  avatar: string option [@default None];
  associated: profile_associated option [@default None];
  viewer: actor_viewer_state option [@default None];
  labels: Com_atproto_label_defs.label list option [@default None];
  created_at: string option [@key "createdAt"] [@default None];
  verification: verification_state option [@default None];
  status: status_view option [@default None];
}
and actor_viewer_state = {
  muted: bool option [@default None];
  muted_by_list: list_view_basic option [@key "mutedByList"] [@default None];
  blocked_by: bool option [@key "blockedBy"] [@default None];
  blocking: string option [@default None];
  blocking_by_list: list_view_basic option [@key "blockingByList"] [@default None];
  following: string option [@default None];
  followed_by: string option [@key "followedBy"] [@default None];
  known_followers: known_followers option [@key "knownFollowers"] [@default None];
  activity_subscription: App_bsky_notification_defs.activity_subscription option [@key "activitySubscription"] [@default None];
}
and known_followers = {
  count: int;
  followers: profile_view_basic list;
}

let rec profile_view_basic_of_yojson json =
  let open Yojson.Safe.Util in
  try
    let did = json |> member "did" |> to_string in
    let handle = json |> member "handle" |> to_string in
    let display_name = json |> member "displayName" |> to_option to_string in
    let avatar = json |> member "avatar" |> to_option to_string in
    let associated = json |> member "associated" |> to_option (fun x -> match profile_associated_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    let viewer = json |> member "viewer" |> to_option (fun x -> match actor_viewer_state_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    let labels = json |> member "labels" |> to_option (fun j -> to_list j |> List.filter_map (fun x -> match Com_atproto_label_defs.label_of_yojson x with Ok v -> Some v | _ -> None)) in
    let created_at = json |> member "createdAt" |> to_option to_string in
    let verification = json |> member "verification" |> to_option (fun x -> match verification_state_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    let status = json |> member "status" |> to_option (fun x -> match status_view_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    Ok { did; handle; display_name; avatar; associated; viewer; labels; created_at; verification; status }
  with e -> Error (Printexc.to_string e)

and actor_viewer_state_of_yojson json =
  let open Yojson.Safe.Util in
  try
    let muted = json |> member "muted" |> to_option to_bool in
    let muted_by_list = json |> member "mutedByList" |> to_option (fun x -> match list_view_basic_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    let blocked_by = json |> member "blockedBy" |> to_option to_bool in
    let blocking = json |> member "blocking" |> to_option to_string in
    let blocking_by_list = json |> member "blockingByList" |> to_option (fun x -> match list_view_basic_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    let following = json |> member "following" |> to_option to_string in
    let followed_by = json |> member "followedBy" |> to_option to_string in
    let known_followers = json |> member "knownFollowers" |> to_option (fun x -> match known_followers_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    let activity_subscription = json |> member "activitySubscription" |> to_option (fun x -> match App_bsky_notification_defs.activity_subscription_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    Ok { muted; muted_by_list; blocked_by; blocking; blocking_by_list; following; followed_by; known_followers; activity_subscription }
  with e -> Error (Printexc.to_string e)

and known_followers_of_yojson json =
  let open Yojson.Safe.Util in
  try
    let count = json |> member "count" |> to_int in
    let followers = json |> member "followers" |> (fun j -> to_list j |> List.filter_map (fun x -> match profile_view_basic_of_yojson x with Ok v -> Some v | _ -> None)) in
    Ok { count; followers }
  with e -> Error (Printexc.to_string e)

and profile_view_basic_to_yojson (r : profile_view_basic) =
  `Assoc [
    ("did", (fun s -> `String s) r.did);
    ("handle", (fun s -> `String s) r.handle);
    ("displayName", match r.display_name with Some v -> (fun s -> `String s) v | None -> `Null);
    ("avatar", match r.avatar with Some v -> (fun s -> `String s) v | None -> `Null);
    ("associated", match r.associated with Some v -> profile_associated_to_yojson v | None -> `Null);
    ("viewer", match r.viewer with Some v -> actor_viewer_state_to_yojson v | None -> `Null);
    ("labels", match r.labels with Some v -> (fun l -> `List (List.map Com_atproto_label_defs.label_to_yojson l)) v | None -> `Null);
    ("createdAt", match r.created_at with Some v -> (fun s -> `String s) v | None -> `Null);
    ("verification", match r.verification with Some v -> verification_state_to_yojson v | None -> `Null);
    ("status", match r.status with Some v -> status_view_to_yojson v | None -> `Null)
  ]

and actor_viewer_state_to_yojson (r : actor_viewer_state) =
  `Assoc [
    ("muted", match r.muted with Some v -> (fun b -> `Bool b) v | None -> `Null);
    ("mutedByList", match r.muted_by_list with Some v -> list_view_basic_to_yojson v | None -> `Null);
    ("blockedBy", match r.blocked_by with Some v -> (fun b -> `Bool b) v | None -> `Null);
    ("blocking", match r.blocking with Some v -> (fun s -> `String s) v | None -> `Null);
    ("blockingByList", match r.blocking_by_list with Some v -> list_view_basic_to_yojson v | None -> `Null);
    ("following", match r.following with Some v -> (fun s -> `String s) v | None -> `Null);
    ("followedBy", match r.followed_by with Some v -> (fun s -> `String s) v | None -> `Null);
    ("knownFollowers", match r.known_followers with Some v -> known_followers_to_yojson v | None -> `Null);
    ("activitySubscription", match r.activity_subscription with Some v -> App_bsky_notification_defs.activity_subscription_to_yojson v | None -> `Null)
  ]

and known_followers_to_yojson (r : known_followers) =
  `Assoc [
    ("count", (fun i -> `Int i) r.count);
    ("followers", (fun l -> `List (List.map profile_view_basic_to_yojson l)) r.followers)
  ]

type profile_view =
  {
    did: string;
    handle: string;
    display_name: string option [@key "displayName"] [@default None];
    description: string option [@default None];
    avatar: string option [@default None];
    associated: profile_associated option [@default None];
    indexed_at: string option [@key "indexedAt"] [@default None];
    created_at: string option [@key "createdAt"] [@default None];
    viewer: actor_viewer_state option [@default None];
    labels: Com_atproto_label_defs.label list option [@default None];
    verification: verification_state option [@default None];
    status: status_view option [@default None];
  }
[@@deriving yojson {strict= false}]

type starter_pack_view_basic =
  {
    uri: string;
    cid: string;
    record: Yojson.Safe.t;
    creator: profile_view_basic;
    list_item_count: int option [@key "listItemCount"] [@default None];
    joined_week_count: int option [@key "joinedWeekCount"] [@default None];
    joined_all_time_count: int option [@key "joinedAllTimeCount"] [@default None];
    labels: Com_atproto_label_defs.label list option [@default None];
    indexed_at: string [@key "indexedAt"];
  }
[@@deriving yojson {strict= false}]

type profile_view_detailed =
  {
    did: string;
    handle: string;
    display_name: string option [@key "displayName"] [@default None];
    description: string option [@default None];
    avatar: string option [@default None];
    banner: string option [@default None];
    followers_count: int option [@key "followersCount"] [@default None];
    follows_count: int option [@key "followsCount"] [@default None];
    posts_count: int option [@key "postsCount"] [@default None];
    associated: profile_associated option [@default None];
    joined_via_starter_pack: starter_pack_view_basic option [@key "joinedViaStarterPack"] [@default None];
    indexed_at: string option [@key "indexedAt"] [@default None];
    created_at: string option [@key "createdAt"] [@default None];
    viewer: actor_viewer_state option [@default None];
    labels: Com_atproto_label_defs.label list option [@default None];
    pinned_post: Com_atproto_repo_strongRef.main option [@key "pinnedPost"] [@default None];
    verification: verification_state option [@default None];
    status: status_view option [@default None];
  }
[@@deriving yojson {strict= false}]

type verification_prefs =
  {
    hide_badges: bool option [@key "hideBadges"] [@default None];
  }
[@@deriving yojson {strict= false}]

type postgate_embedding_rules_item =
  | PostgateDisableRule of App_bsky_feed_postgate.disable_rule
  | Unknown of Yojson.Safe.t

let postgate_embedding_rules_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.postgate#disableRule" ->
        (match App_bsky_feed_postgate.disable_rule_of_yojson json with
         | Ok v -> Ok (PostgateDisableRule v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let postgate_embedding_rules_item_to_yojson = function
  | PostgateDisableRule v ->
      (match App_bsky_feed_postgate.disable_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.postgate#disableRule") :: fields)
       | other -> other)
  | Unknown j -> j

type threadgate_allow_rules_item =
  | ThreadgateMentionRule of App_bsky_feed_threadgate.mention_rule
  | ThreadgateFollowerRule of App_bsky_feed_threadgate.follower_rule
  | ThreadgateFollowingRule of App_bsky_feed_threadgate.following_rule
  | ThreadgateListRule of App_bsky_feed_threadgate.list_rule
  | Unknown of Yojson.Safe.t

let threadgate_allow_rules_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.threadgate#mentionRule" ->
        (match App_bsky_feed_threadgate.mention_rule_of_yojson json with
         | Ok v -> Ok (ThreadgateMentionRule v)
         | Error e -> Error e)
    | "app.bsky.feed.threadgate#followerRule" ->
        (match App_bsky_feed_threadgate.follower_rule_of_yojson json with
         | Ok v -> Ok (ThreadgateFollowerRule v)
         | Error e -> Error e)
    | "app.bsky.feed.threadgate#followingRule" ->
        (match App_bsky_feed_threadgate.following_rule_of_yojson json with
         | Ok v -> Ok (ThreadgateFollowingRule v)
         | Error e -> Error e)
    | "app.bsky.feed.threadgate#listRule" ->
        (match App_bsky_feed_threadgate.list_rule_of_yojson json with
         | Ok v -> Ok (ThreadgateListRule v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let threadgate_allow_rules_item_to_yojson = function
  | ThreadgateMentionRule v ->
      (match App_bsky_feed_threadgate.mention_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.threadgate#mentionRule") :: fields)
       | other -> other)
  | ThreadgateFollowerRule v ->
      (match App_bsky_feed_threadgate.follower_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.threadgate#followerRule") :: fields)
       | other -> other)
  | ThreadgateFollowingRule v ->
      (match App_bsky_feed_threadgate.following_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.threadgate#followingRule") :: fields)
       | other -> other)
  | ThreadgateListRule v ->
      (match App_bsky_feed_threadgate.list_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.threadgate#listRule") :: fields)
       | other -> other)
  | Unknown j -> j

type post_interaction_settings_pref =
  {
    threadgate_allow_rules: threadgate_allow_rules_item list option [@key "threadgateAllowRules"] [@default None];
    postgate_embedding_rules: postgate_embedding_rules_item list option [@key "postgateEmbeddingRules"] [@default None];
  }
[@@deriving yojson {strict= false}]

type labeler_pref_item =
  {
    did: string;
  }
[@@deriving yojson {strict= false}]

type labelers_pref =
  {
    labelers: labeler_pref_item list;
  }
[@@deriving yojson {strict= false}]

type nux =
  {
    id: string;
    completed: bool;
    data: string option [@default None];
    expires_at: string option [@key "expiresAt"] [@default None];
  }
[@@deriving yojson {strict= false}]

type bsky_app_progress_guide =
  {
    guide: string;
  }
[@@deriving yojson {strict= false}]

type bsky_app_state_pref =
  {
    active_progress_guide: bsky_app_progress_guide option [@key "activeProgressGuide"] [@default None];
    queued_nudges: string list option [@key "queuedNudges"] [@default None];
    nuxs: nux list option [@default None];
  }
[@@deriving yojson {strict= false}]

type hidden_posts_pref =
  {
    items: string list;
  }
[@@deriving yojson {strict= false}]

(** string type with known values *)
type muted_word_target = string
let muted_word_target_of_yojson = function
  | `String s -> Ok s
  | _ -> Error "muted_word_target: expected string"
let muted_word_target_to_yojson s = `String s

type muted_word =
  {
    id: string option [@default None];
    value: string;
    targets: muted_word_target list;
    actor_target: string option [@key "actorTarget"] [@default None];
    expires_at: string option [@key "expiresAt"] [@default None];
  }
[@@deriving yojson {strict= false}]

type muted_words_pref =
  {
    items: muted_word list;
  }
[@@deriving yojson {strict= false}]

type interests_pref =
  {
    tags: string list;
  }
[@@deriving yojson {strict= false}]

type thread_view_pref =
  {
    sort: string option [@default None];
    prioritize_followed_users: bool option [@key "prioritizeFollowedUsers"] [@default None];
  }
[@@deriving yojson {strict= false}]

type feed_view_pref =
  {
    feed: string;
    hide_replies: bool option [@key "hideReplies"] [@default None];
    hide_replies_by_unfollowed: bool option [@key "hideRepliesByUnfollowed"] [@default None];
    hide_replies_by_like_count: int option [@key "hideRepliesByLikeCount"] [@default None];
    hide_reposts: bool option [@key "hideReposts"] [@default None];
    hide_quote_posts: bool option [@key "hideQuotePosts"] [@default None];
  }
[@@deriving yojson {strict= false}]

type personal_details_pref =
  {
    birth_date: string option [@key "birthDate"] [@default None];
  }
[@@deriving yojson {strict= false}]

type saved_feed =
  {
    id: string;
    type_: string [@key "type"];
    value: string;
    pinned: bool;
  }
[@@deriving yojson {strict= false}]

type saved_feeds_pref_v2 =
  {
    items: saved_feed list;
  }
[@@deriving yojson {strict= false}]

type saved_feeds_pref =
  {
    pinned: string list;
    saved: string list;
    timeline_index: int option [@key "timelineIndex"] [@default None];
  }
[@@deriving yojson {strict= false}]

type content_label_pref =
  {
    labeler_did: string option [@key "labelerDid"] [@default None];
    label: string;
    visibility: string;
  }
[@@deriving yojson {strict= false}]

type adult_content_pref =
  {
    enabled: bool;
  }
[@@deriving yojson {strict= false}]

type preferences_item =
  | AdultContentPref of adult_content_pref
  | ContentLabelPref of content_label_pref
  | SavedFeedsPref of saved_feeds_pref
  | SavedFeedsPrefV2 of saved_feeds_pref_v2
  | PersonalDetailsPref of personal_details_pref
  | FeedViewPref of feed_view_pref
  | ThreadViewPref of thread_view_pref
  | InterestsPref of interests_pref
  | MutedWordsPref of muted_words_pref
  | HiddenPostsPref of hidden_posts_pref
  | BskyAppStatePref of bsky_app_state_pref
  | LabelersPref of labelers_pref
  | PostInteractionSettingsPref of post_interaction_settings_pref
  | VerificationPrefs of verification_prefs
  | Unknown of Yojson.Safe.t

let preferences_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.actor.defs#adultContentPref" ->
        (match adult_content_pref_of_yojson json with
         | Ok v -> Ok (AdultContentPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#contentLabelPref" ->
        (match content_label_pref_of_yojson json with
         | Ok v -> Ok (ContentLabelPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#savedFeedsPref" ->
        (match saved_feeds_pref_of_yojson json with
         | Ok v -> Ok (SavedFeedsPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#savedFeedsPrefV2" ->
        (match saved_feeds_pref_v2_of_yojson json with
         | Ok v -> Ok (SavedFeedsPrefV2 v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#personalDetailsPref" ->
        (match personal_details_pref_of_yojson json with
         | Ok v -> Ok (PersonalDetailsPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#feedViewPref" ->
        (match feed_view_pref_of_yojson json with
         | Ok v -> Ok (FeedViewPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#threadViewPref" ->
        (match thread_view_pref_of_yojson json with
         | Ok v -> Ok (ThreadViewPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#interestsPref" ->
        (match interests_pref_of_yojson json with
         | Ok v -> Ok (InterestsPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#mutedWordsPref" ->
        (match muted_words_pref_of_yojson json with
         | Ok v -> Ok (MutedWordsPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#hiddenPostsPref" ->
        (match hidden_posts_pref_of_yojson json with
         | Ok v -> Ok (HiddenPostsPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#bskyAppStatePref" ->
        (match bsky_app_state_pref_of_yojson json with
         | Ok v -> Ok (BskyAppStatePref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#labelersPref" ->
        (match labelers_pref_of_yojson json with
         | Ok v -> Ok (LabelersPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#postInteractionSettingsPref" ->
        (match post_interaction_settings_pref_of_yojson json with
         | Ok v -> Ok (PostInteractionSettingsPref v)
         | Error e -> Error e)
    | "app.bsky.actor.defs#verificationPrefs" ->
        (match verification_prefs_of_yojson json with
         | Ok v -> Ok (VerificationPrefs v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let preferences_item_to_yojson = function
  | AdultContentPref v ->
      (match adult_content_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#adultContentPref") :: fields)
       | other -> other)
  | ContentLabelPref v ->
      (match content_label_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#contentLabelPref") :: fields)
       | other -> other)
  | SavedFeedsPref v ->
      (match saved_feeds_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#savedFeedsPref") :: fields)
       | other -> other)
  | SavedFeedsPrefV2 v ->
      (match saved_feeds_pref_v2_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#savedFeedsPrefV2") :: fields)
       | other -> other)
  | PersonalDetailsPref v ->
      (match personal_details_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#personalDetailsPref") :: fields)
       | other -> other)
  | FeedViewPref v ->
      (match feed_view_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#feedViewPref") :: fields)
       | other -> other)
  | ThreadViewPref v ->
      (match thread_view_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#threadViewPref") :: fields)
       | other -> other)
  | InterestsPref v ->
      (match interests_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#interestsPref") :: fields)
       | other -> other)
  | MutedWordsPref v ->
      (match muted_words_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#mutedWordsPref") :: fields)
       | other -> other)
  | HiddenPostsPref v ->
      (match hidden_posts_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#hiddenPostsPref") :: fields)
       | other -> other)
  | BskyAppStatePref v ->
      (match bsky_app_state_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#bskyAppStatePref") :: fields)
       | other -> other)
  | LabelersPref v ->
      (match labelers_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#labelersPref") :: fields)
       | other -> other)
  | PostInteractionSettingsPref v ->
      (match post_interaction_settings_pref_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#postInteractionSettingsPref") :: fields)
       | other -> other)
  | VerificationPrefs v ->
      (match verification_prefs_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.actor.defs#verificationPrefs") :: fields)
       | other -> other)
  | Unknown j -> j

type preferences = preferences_item list
let preferences_of_yojson json =
  let open Yojson.Safe.Util in
  Ok (to_list json |> List.filter_map (fun x -> match preferences_item_of_yojson x with Ok v -> Some v | _ -> None))
let preferences_to_yojson l = `List (List.map preferences_item_to_yojson l)

type record_main =
  {
    record: Com_atproto_repo_strongRef.main;
  }
[@@deriving yojson {strict= false}]

type labeler_viewer_state =
  {
    like: string option [@default None];
  }
[@@deriving yojson {strict= false}]

type labeler_view =
  {
    uri: string;
    cid: string;
    creator: profile_view;
    like_count: int option [@key "likeCount"] [@default None];
    viewer: labeler_viewer_state option [@default None];
    indexed_at: string [@key "indexedAt"];
    labels: Com_atproto_label_defs.label list option [@default None];
  }
[@@deriving yojson {strict= false}]

type list_view =
  {
    uri: string;
    cid: string;
    creator: profile_view;
    name: string;
    purpose: list_purpose;
    description: string option [@default None];
    description_facets: App_bsky_richtext_facet.main list option [@key "descriptionFacets"] [@default None];
    avatar: string option [@default None];
    list_item_count: int option [@key "listItemCount"] [@default None];
    labels: Com_atproto_label_defs.label list option [@default None];
    viewer: list_viewer_state option [@default None];
    indexed_at: string [@key "indexedAt"];
  }
[@@deriving yojson {strict= false}]

type generator_viewer_state =
  {
    like: string option [@default None];
  }
[@@deriving yojson {strict= false}]

type generator_view =
  {
    uri: string;
    cid: string;
    did: string;
    creator: profile_view;
    display_name: string [@key "displayName"];
    description: string option [@default None];
    description_facets: App_bsky_richtext_facet.main list option [@key "descriptionFacets"] [@default None];
    avatar: string option [@default None];
    like_count: int option [@key "likeCount"] [@default None];
    accepts_interactions: bool option [@key "acceptsInteractions"] [@default None];
    labels: Com_atproto_label_defs.label list option [@default None];
    viewer: generator_viewer_state option [@default None];
    content_mode: string option [@key "contentMode"] [@default None];
    indexed_at: string [@key "indexedAt"];
  }
[@@deriving yojson {strict= false}]

type view_detached =
  {
    uri: string;
    detached: bool;
  }
[@@deriving yojson {strict= false}]

type blocked_author =
  {
    did: string;
    viewer: actor_viewer_state option [@default None];
  }
[@@deriving yojson {strict= false}]

type view_blocked =
  {
    uri: string;
    blocked: bool;
    author: blocked_author;
  }
[@@deriving yojson {strict= false}]

type view_not_found =
  {
    uri: string;
    not_found: bool [@key "notFound"];
  }
[@@deriving yojson {strict= false}]

type record_with_media_media =
  | Images of App_bsky_embed_images.main
  | Video of App_bsky_embed_video.main
  | External of App_bsky_embed_external.main
  | Unknown of Yojson.Safe.t

let record_with_media_media_of_yojson json =
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
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let record_with_media_media_to_yojson = function
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
  | Unknown j -> j

type embeds_item =
  | ImagesView of App_bsky_embed_images.view
  | VideoView of App_bsky_embed_video.view
  | ExternalView of App_bsky_embed_external.view
  | RecordView of record_view
  | RecordWithMediaView of record_with_media_view
  | Unknown of Yojson.Safe.t
and record =
  | ViewRecord of view_record
  | ViewNotFound of view_not_found
  | ViewBlocked of view_blocked
  | ViewDetached of view_detached
  | DefsGeneratorView of generator_view
  | DefsListView of list_view
  | DefsLabelerView of labeler_view
  | DefsStarterPackViewBasic of starter_pack_view_basic
  | Unknown of Yojson.Safe.t
and record_view = {
  record: record;
}
and view_record = {
  uri: string;
  cid: string;
  author: profile_view_basic;
  value: Yojson.Safe.t;
  labels: Com_atproto_label_defs.label list option [@default None];
  reply_count: int option [@key "replyCount"] [@default None];
  repost_count: int option [@key "repostCount"] [@default None];
  like_count: int option [@key "likeCount"] [@default None];
  quote_count: int option [@key "quoteCount"] [@default None];
  embeds: embeds_item list option [@default None];
  indexed_at: string [@key "indexedAt"];
}
and record_with_media_view = {
  record: record_view;
  media: record_with_media_media;
}

let rec embeds_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.embed.images#view" ->
        (match App_bsky_embed_images.view_of_yojson json with
         | Ok v -> Ok (ImagesView v)
         | Error e -> Error e)
    | "app.bsky.embed.video#view" ->
        (match App_bsky_embed_video.view_of_yojson json with
         | Ok v -> Ok (VideoView v)
         | Error e -> Error e)
    | "app.bsky.embed.external#view" ->
        (match App_bsky_embed_external.view_of_yojson json with
         | Ok v -> Ok (ExternalView v)
         | Error e -> Error e)
    | "app.bsky.embed.record#view" ->
        (match record_view_of_yojson json with
         | Ok v -> Ok (RecordView v)
         | Error e -> Error e)
    | "app.bsky.embed.recordWithMedia#view" ->
        (match record_with_media_view_of_yojson json with
         | Ok v -> Ok (RecordWithMediaView v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

and record_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.embed.record#viewRecord" ->
        (match view_record_of_yojson json with
         | Ok v -> Ok (ViewRecord v)
         | Error e -> Error e)
    | "app.bsky.embed.record#viewNotFound" ->
        (match view_not_found_of_yojson json with
         | Ok v -> Ok (ViewNotFound v)
         | Error e -> Error e)
    | "app.bsky.embed.record#viewBlocked" ->
        (match view_blocked_of_yojson json with
         | Ok v -> Ok (ViewBlocked v)
         | Error e -> Error e)
    | "app.bsky.embed.record#viewDetached" ->
        (match view_detached_of_yojson json with
         | Ok v -> Ok (ViewDetached v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#generatorView" ->
        (match generator_view_of_yojson json with
         | Ok v -> Ok (DefsGeneratorView v)
         | Error e -> Error e)
    | "app.bsky.graph.defs#listView" ->
        (match list_view_of_yojson json with
         | Ok v -> Ok (DefsListView v)
         | Error e -> Error e)
    | "app.bsky.labeler.defs#labelerView" ->
        (match labeler_view_of_yojson json with
         | Ok v -> Ok (DefsLabelerView v)
         | Error e -> Error e)
    | "app.bsky.graph.defs#starterPackViewBasic" ->
        (match starter_pack_view_basic_of_yojson json with
         | Ok v -> Ok (DefsStarterPackViewBasic v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

and record_view_of_yojson json =
  let open Yojson.Safe.Util in
  try
    let record = json |> member "record" |> record_of_yojson |> Result.get_ok in
    Ok { record }
  with e -> Error (Printexc.to_string e)

and view_record_of_yojson json =
  let open Yojson.Safe.Util in
  try
    let uri = json |> member "uri" |> to_string in
    let cid = json |> member "cid" |> to_string in
    let author = json |> member "author" |> profile_view_basic_of_yojson |> Result.get_ok in
    let value = json |> member "value" |> (fun j -> j) in
    let labels = json |> member "labels" |> to_option (fun j -> to_list j |> List.filter_map (fun x -> match Com_atproto_label_defs.label_of_yojson x with Ok v -> Some v | _ -> None)) in
    let reply_count = json |> member "replyCount" |> to_option to_int in
    let repost_count = json |> member "repostCount" |> to_option to_int in
    let like_count = json |> member "likeCount" |> to_option to_int in
    let quote_count = json |> member "quoteCount" |> to_option to_int in
    let embeds = json |> member "embeds" |> to_option (fun j -> to_list j |> List.filter_map (fun x -> match embeds_item_of_yojson x with Ok v -> Some v | _ -> None)) in
    let indexed_at = json |> member "indexedAt" |> to_string in
    Ok { uri; cid; author; value; labels; reply_count; repost_count; like_count; quote_count; embeds; indexed_at }
  with e -> Error (Printexc.to_string e)

and record_with_media_view_of_yojson json =
  let open Yojson.Safe.Util in
  try
    let record = json |> member "record" |> record_view_of_yojson |> Result.get_ok in
    let media = json |> member "media" |> record_with_media_media_of_yojson |> Result.get_ok in
    Ok { record; media }
  with e -> Error (Printexc.to_string e)

and embeds_item_to_yojson = function
  | ImagesView v ->
      (match App_bsky_embed_images.view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.images#view") :: fields)
       | other -> other)
  | VideoView v ->
      (match App_bsky_embed_video.view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.video#view") :: fields)
       | other -> other)
  | ExternalView v ->
      (match App_bsky_embed_external.view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.external#view") :: fields)
       | other -> other)
  | RecordView v ->
      (match record_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.record#view") :: fields)
       | other -> other)
  | RecordWithMediaView v ->
      (match record_with_media_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.recordWithMedia#view") :: fields)
       | other -> other)
  | Unknown j -> j

and record_to_yojson = function
  | ViewRecord v ->
      (match view_record_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.record#viewRecord") :: fields)
       | other -> other)
  | ViewNotFound v ->
      (match view_not_found_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.record#viewNotFound") :: fields)
       | other -> other)
  | ViewBlocked v ->
      (match view_blocked_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.record#viewBlocked") :: fields)
       | other -> other)
  | ViewDetached v ->
      (match view_detached_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.record#viewDetached") :: fields)
       | other -> other)
  | DefsGeneratorView v ->
      (match generator_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#generatorView") :: fields)
       | other -> other)
  | DefsListView v ->
      (match list_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.graph.defs#listView") :: fields)
       | other -> other)
  | DefsLabelerView v ->
      (match labeler_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.labeler.defs#labelerView") :: fields)
       | other -> other)
  | DefsStarterPackViewBasic v ->
      (match starter_pack_view_basic_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.graph.defs#starterPackViewBasic") :: fields)
       | other -> other)
  | Unknown j -> j

and record_view_to_yojson (r : record_view) =
  `Assoc [
    ("record", record_to_yojson r.record)
  ]

and view_record_to_yojson (r : view_record) =
  `Assoc [
    ("uri", (fun s -> `String s) r.uri);
    ("cid", (fun s -> `String s) r.cid);
    ("author", profile_view_basic_to_yojson r.author);
    ("value", (fun j -> j) r.value);
    ("labels", match r.labels with Some v -> (fun l -> `List (List.map Com_atproto_label_defs.label_to_yojson l)) v | None -> `Null);
    ("replyCount", match r.reply_count with Some v -> (fun i -> `Int i) v | None -> `Null);
    ("repostCount", match r.repost_count with Some v -> (fun i -> `Int i) v | None -> `Null);
    ("likeCount", match r.like_count with Some v -> (fun i -> `Int i) v | None -> `Null);
    ("quoteCount", match r.quote_count with Some v -> (fun i -> `Int i) v | None -> `Null);
    ("embeds", match r.embeds with Some v -> (fun l -> `List (List.map embeds_item_to_yojson l)) v | None -> `Null);
    ("indexedAt", (fun s -> `String s) r.indexed_at)
  ]

and record_with_media_view_to_yojson (r : record_with_media_view) =
  `Assoc [
    ("record", record_view_to_yojson r.record);
    ("media", record_with_media_media_to_yojson r.media)
  ]

type record_with_media_main =
  {
    record: record_main;
    media: record_with_media_media;
  }
[@@deriving yojson {strict= false}]

type threadgate_view =
  {
    uri: string option [@default None];
    cid: string option [@default None];
    record: Yojson.Safe.t option [@default None];
    lists: list_view_basic list option [@default None];
  }
[@@deriving yojson {strict= false}]

type feed_viewer_state =
  {
    repost: string option [@default None];
    like: string option [@default None];
    thread_muted: bool option [@key "threadMuted"] [@default None];
    reply_disabled: bool option [@key "replyDisabled"] [@default None];
    embedding_disabled: bool option [@key "embeddingDisabled"] [@default None];
    pinned: bool option [@default None];
  }
[@@deriving yojson {strict= false}]

type feed_embed =
  | ImagesView of App_bsky_embed_images.view
  | VideoView of App_bsky_embed_video.view
  | ExternalView of App_bsky_embed_external.view
  | RecordView of record_view
  | RecordWithMediaView of record_with_media_view
  | Unknown of Yojson.Safe.t

let feed_embed_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.embed.images#view" ->
        (match App_bsky_embed_images.view_of_yojson json with
         | Ok v -> Ok (ImagesView v)
         | Error e -> Error e)
    | "app.bsky.embed.video#view" ->
        (match App_bsky_embed_video.view_of_yojson json with
         | Ok v -> Ok (VideoView v)
         | Error e -> Error e)
    | "app.bsky.embed.external#view" ->
        (match App_bsky_embed_external.view_of_yojson json with
         | Ok v -> Ok (ExternalView v)
         | Error e -> Error e)
    | "app.bsky.embed.record#view" ->
        (match record_view_of_yojson json with
         | Ok v -> Ok (RecordView v)
         | Error e -> Error e)
    | "app.bsky.embed.recordWithMedia#view" ->
        (match record_with_media_view_of_yojson json with
         | Ok v -> Ok (RecordWithMediaView v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let feed_embed_to_yojson = function
  | ImagesView v ->
      (match App_bsky_embed_images.view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.images#view") :: fields)
       | other -> other)
  | VideoView v ->
      (match App_bsky_embed_video.view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.video#view") :: fields)
       | other -> other)
  | ExternalView v ->
      (match App_bsky_embed_external.view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.external#view") :: fields)
       | other -> other)
  | RecordView v ->
      (match record_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.record#view") :: fields)
       | other -> other)
  | RecordWithMediaView v ->
      (match record_with_media_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.embed.recordWithMedia#view") :: fields)
       | other -> other)
  | Unknown j -> j

type post_view =
  {
    uri: string;
    cid: string;
    author: profile_view_basic;
    record: Yojson.Safe.t;
    embed: embeds_item option [@default None];
    reply_count: int option [@key "replyCount"] [@default None];
    repost_count: int option [@key "repostCount"] [@default None];
    like_count: int option [@key "likeCount"] [@default None];
    quote_count: int option [@key "quoteCount"] [@default None];
    indexed_at: string [@key "indexedAt"];
    viewer: feed_viewer_state option [@default None];
    labels: Com_atproto_label_defs.label list option [@default None];
    threadgate: threadgate_view option [@default None];
  }
[@@deriving yojson {strict= false}]

type thread_context =
  {
    root_author_like: string option [@key "rootAuthorLike"] [@default None];
  }
[@@deriving yojson {strict= false}]

type reason_pin = unit
let reason_pin_of_yojson _ = Ok ()
let reason_pin_to_yojson () = `Assoc []

type reason_repost =
  {
    by: profile_view_basic;
    uri: string option [@default None];
    cid: string option [@default None];
    indexed_at: string [@key "indexedAt"];
  }
[@@deriving yojson {strict= false}]

type blocked_post =
  {
    uri: string;
    blocked: bool;
    author: blocked_author;
  }
[@@deriving yojson {strict= false}]

type not_found_post =
  {
    uri: string;
    not_found: bool [@key "notFound"];
  }
[@@deriving yojson {strict= false}]

type feed_parent =
  | PostView of post_view
  | NotFoundPost of not_found_post
  | BlockedPost of blocked_post
  | Unknown of Yojson.Safe.t

let feed_parent_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.defs#postView" ->
        (match post_view_of_yojson json with
         | Ok v -> Ok (PostView v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#notFoundPost" ->
        (match not_found_post_of_yojson json with
         | Ok v -> Ok (NotFoundPost v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#blockedPost" ->
        (match blocked_post_of_yojson json with
         | Ok v -> Ok (BlockedPost v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let feed_parent_to_yojson = function
  | PostView v ->
      (match post_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#postView") :: fields)
       | other -> other)
  | NotFoundPost v ->
      (match not_found_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#notFoundPost") :: fields)
       | other -> other)
  | BlockedPost v ->
      (match blocked_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#blockedPost") :: fields)
       | other -> other)
  | Unknown j -> j

type root =
  | PostView of post_view
  | NotFoundPost of not_found_post
  | BlockedPost of blocked_post
  | Unknown of Yojson.Safe.t

let root_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.defs#postView" ->
        (match post_view_of_yojson json with
         | Ok v -> Ok (PostView v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#notFoundPost" ->
        (match not_found_post_of_yojson json with
         | Ok v -> Ok (NotFoundPost v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#blockedPost" ->
        (match blocked_post_of_yojson json with
         | Ok v -> Ok (BlockedPost v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let root_to_yojson = function
  | PostView v ->
      (match post_view_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#postView") :: fields)
       | other -> other)
  | NotFoundPost v ->
      (match not_found_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#notFoundPost") :: fields)
       | other -> other)
  | BlockedPost v ->
      (match blocked_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#blockedPost") :: fields)
       | other -> other)
  | Unknown j -> j

type reply_ref =
  {
    root: feed_parent;
    parent: feed_parent;
    grandparent_author: profile_view_basic option [@key "grandparentAuthor"] [@default None];
  }
[@@deriving yojson {strict= false}]

type feed_reason =
  | ReasonRepost of reason_repost
  | ReasonPin of reason_pin
  | Unknown of Yojson.Safe.t

let feed_reason_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.defs#reasonRepost" ->
        (match reason_repost_of_yojson json with
         | Ok v -> Ok (ReasonRepost v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#reasonPin" ->
        (match reason_pin_of_yojson json with
         | Ok v -> Ok (ReasonPin v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let feed_reason_to_yojson = function
  | ReasonRepost v ->
      (match reason_repost_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#reasonRepost") :: fields)
       | other -> other)
  | ReasonPin v ->
      (match reason_pin_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#reasonPin") :: fields)
       | other -> other)
  | Unknown j -> j

type feed_view_post =
  {
    post: post_view;
    reply: reply_ref option [@default None];
    reason: feed_reason option [@default None];
    feed_context: string option [@key "feedContext"] [@default None];
    req_id: string option [@key "reqId"] [@default None];
  }
[@@deriving yojson {strict= false}]

type replies_item =
  | ThreadViewPost of thread_view_post
  | NotFoundPost of not_found_post
  | BlockedPost of blocked_post
  | Unknown of Yojson.Safe.t
and thread_view_post = {
  post: post_view;
  parent: replies_item option [@default None];
  replies: replies_item list option [@default None];
  thread_context: thread_context option [@key "threadContext"] [@default None];
}

let rec replies_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.defs#threadViewPost" ->
        (match thread_view_post_of_yojson json with
         | Ok v -> Ok (ThreadViewPost v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#notFoundPost" ->
        (match not_found_post_of_yojson json with
         | Ok v -> Ok (NotFoundPost v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#blockedPost" ->
        (match blocked_post_of_yojson json with
         | Ok v -> Ok (BlockedPost v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

and thread_view_post_of_yojson json =
  let open Yojson.Safe.Util in
  try
    let post = json |> member "post" |> post_view_of_yojson |> Result.get_ok in
    let parent = json |> member "parent" |> to_option (fun x -> match replies_item_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    let replies = json |> member "replies" |> to_option (fun j -> to_list j |> List.filter_map (fun x -> match replies_item_of_yojson x with Ok v -> Some v | _ -> None)) in
    let thread_context = json |> member "threadContext" |> to_option (fun x -> match thread_context_of_yojson x with Ok v -> Some v | _ -> None) |> Option.join in
    Ok { post; parent; replies; thread_context }
  with e -> Error (Printexc.to_string e)

and replies_item_to_yojson = function
  | ThreadViewPost v ->
      (match thread_view_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#threadViewPost") :: fields)
       | other -> other)
  | NotFoundPost v ->
      (match not_found_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#notFoundPost") :: fields)
       | other -> other)
  | BlockedPost v ->
      (match blocked_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#blockedPost") :: fields)
       | other -> other)
  | Unknown j -> j

and thread_view_post_to_yojson (r : thread_view_post) =
  `Assoc [
    ("post", post_view_to_yojson r.post);
    ("parent", match r.parent with Some v -> replies_item_to_yojson v | None -> `Null);
    ("replies", match r.replies with Some v -> (fun l -> `List (List.map replies_item_to_yojson l)) v | None -> `Null);
    ("threadContext", match r.thread_context with Some v -> thread_context_to_yojson v | None -> `Null)
  ]

type skeleton_reason_pin = unit
let skeleton_reason_pin_of_yojson _ = Ok ()
let skeleton_reason_pin_to_yojson () = `Assoc []

type skeleton_reason_repost =
  {
    repost: string;
  }
[@@deriving yojson {strict= false}]

type skeleton_feed_post =
  {
    post: string;
    reason: feed_reason option [@default None];
    feed_context: string option [@key "feedContext"] [@default None];
  }
[@@deriving yojson {strict= false}]

type interaction =
  {
    item: string option [@default None];
    event: string option [@default None];
    feed_context: string option [@key "feedContext"] [@default None];
    req_id: string option [@key "reqId"] [@default None];
  }
[@@deriving yojson {strict= false}]

(** Request that less content like the given feed item be shown in the feed *)
let request_less = "app.bsky.feed.defs#requestLess"

(** Request that more content like the given feed item be shown in the feed *)
let request_more = "app.bsky.feed.defs#requestMore"

(** User clicked through to the feed item *)
let clickthrough_item = "app.bsky.feed.defs#clickthroughItem"

(** User clicked through to the author of the feed item *)
let clickthrough_author = "app.bsky.feed.defs#clickthroughAuthor"

(** User clicked through to the reposter of the feed item *)
let clickthrough_reposter = "app.bsky.feed.defs#clickthroughReposter"

(** User clicked through to the embedded content of the feed item *)
let clickthrough_embed = "app.bsky.feed.defs#clickthroughEmbed"

(** Declares the feed generator returns any types of posts. *)
let content_mode_unspecified = "app.bsky.feed.defs#contentModeUnspecified"

(** Declares the feed generator returns posts containing app.bsky.embed.video embeds. *)
let content_mode_video = "app.bsky.feed.defs#contentModeVideo"

(** Feed item was seen by user *)
let interaction_seen = "app.bsky.feed.defs#interactionSeen"

(** User liked the feed item *)
let interaction_like = "app.bsky.feed.defs#interactionLike"

(** User reposted the feed item *)
let interaction_repost = "app.bsky.feed.defs#interactionRepost"

(** User replied to the feed item *)
let interaction_reply = "app.bsky.feed.defs#interactionReply"

(** User quoted the feed item *)
let interaction_quote = "app.bsky.feed.defs#interactionQuote"

(** User shared the feed item *)
let interaction_share = "app.bsky.feed.defs#interactionShare"

type list_item_view =
  {
    uri: string;
    subject: profile_view;
  }
[@@deriving yojson {strict= false}]

type starter_pack_view =
  {
    uri: string;
    cid: string;
    record: Yojson.Safe.t;
    creator: profile_view_basic;
    list_: list_view_basic option [@key "list"] [@default None];
    list_items_sample: list_item_view list option [@key "listItemsSample"] [@default None];
    feeds: generator_view list option [@default None];
    joined_week_count: int option [@key "joinedWeekCount"] [@default None];
    joined_all_time_count: int option [@key "joinedAllTimeCount"] [@default None];
    labels: Com_atproto_label_defs.label list option [@default None];
    indexed_at: string [@key "indexedAt"];
  }
[@@deriving yojson {strict= false}]

(** A list of actors to apply an aggregate moderation action (mute/block) on. *)
let modlist = "app.bsky.graph.defs#modlist"

(** A list of actors used for curation purposes such as list feeds or interaction gating. *)
let curatelist = "app.bsky.graph.defs#curatelist"

(** A list of actors used for only for reference purposes such as within a starter pack. *)
let referencelist = "app.bsky.graph.defs#referencelist"

type not_found_actor =
  {
    actor: string;
    not_found: bool [@key "notFound"];
  }
[@@deriving yojson {strict= false}]

type relationship =
  {
    did: string;
    following: string option [@default None];
    followed_by: string option [@key "followedBy"] [@default None];
  }
[@@deriving yojson {strict= false}]

type labeler_policies =
  {
    label_values: Com_atproto_label_defs.label_value list [@key "labelValues"];
    label_value_definitions: Com_atproto_label_defs.label_value_definition list option [@key "labelValueDefinitions"] [@default None];
  }
[@@deriving yojson {strict= false}]

type labeler_view_detailed =
  {
    uri: string;
    cid: string;
    creator: profile_view;
    policies: labeler_policies;
    like_count: int option [@key "likeCount"] [@default None];
    viewer: labeler_viewer_state option [@default None];
    indexed_at: string [@key "indexedAt"];
    labels: Com_atproto_label_defs.label list option [@default None];
    reason_types: Com_atproto_moderation_defs.reason_type list option [@key "reasonTypes"] [@default None];
    subject_types: Com_atproto_moderation_defs.subject_type list option [@key "subjectTypes"] [@default None];
    subject_collections: string list option [@key "subjectCollections"] [@default None];
  }
[@@deriving yojson {strict= false}]

