(* generated from app.bsky.unspecced.defs *)

type skeleton_search_post = {uri: string} [@@deriving yojson {strict= false}]

type skeleton_search_actor = {did: string} [@@deriving yojson {strict= false}]

type skeleton_search_starter_pack = {uri: string}
[@@deriving yojson {strict= false}]

type trending_topic =
  { topic: string
  ; display_name: string option [@key "displayName"] [@default None]
  ; description: string option [@default None]
  ; link: string }
[@@deriving yojson {strict= false}]

type skeleton_trend =
  { topic: string
  ; display_name: string [@key "displayName"]
  ; link: string
  ; started_at: string [@key "startedAt"]
  ; post_count: int [@key "postCount"]
  ; status: string option [@default None]
  ; category: string option [@default None]
  ; dids: string list }
[@@deriving yojson {strict= false}]

type trend_view =
  { topic: string
  ; display_name: string [@key "displayName"]
  ; link: string
  ; started_at: string [@key "startedAt"]
  ; post_count: int [@key "postCount"]
  ; status: string option [@default None]
  ; category: string option [@default None]
  ; actors: App_bsky_actor_defs.profile_view_basic list }
[@@deriving yojson {strict= false}]

type thread_item_post =
  { post: App_bsky_feed_defs.post_view
  ; more_parents: bool [@key "moreParents"]
  ; more_replies: int [@key "moreReplies"]
  ; op_thread: bool [@key "opThread"]
  ; hidden_by_threadgate: bool [@key "hiddenByThreadgate"]
  ; muted_by_viewer: bool [@key "mutedByViewer"] }
[@@deriving yojson {strict= false}]

type thread_item_no_unauthenticated = unit

let thread_item_no_unauthenticated_of_yojson _ = Ok ()

let thread_item_no_unauthenticated_to_yojson () = `Assoc []

type thread_item_not_found = unit

let thread_item_not_found_of_yojson _ = Ok ()

let thread_item_not_found_to_yojson () = `Assoc []

type thread_item_blocked = {author: App_bsky_feed_defs.blocked_author}
[@@deriving yojson {strict= false}]

type age_assurance_state =
  { last_initiated_at: string option [@key "lastInitiatedAt"] [@default None]
  ; status: string }
[@@deriving yojson {strict= false}]

type age_assurance_event =
  { created_at: string [@key "createdAt"]
  ; status: string
  ; attempt_id: string [@key "attemptId"]
  ; email: string option [@default None]
  ; init_ip: string option [@key "initIp"] [@default None]
  ; init_ua: string option [@key "initUa"] [@default None]
  ; complete_ip: string option [@key "completeIp"] [@default None]
  ; complete_ua: string option [@key "completeUa"] [@default None] }
[@@deriving yojson {strict= false}]
