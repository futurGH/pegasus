(* generated from app.bsky.notification.defs *)

type record_deleted = unit

let record_deleted_of_yojson _ = Ok ()

let record_deleted_to_yojson () = `Assoc []

type chat_preference = {include_: string [@key "include"]; push: bool}
[@@deriving yojson {strict= false}]

type filterable_preference =
  {include_: string [@key "include"]; list_: bool [@key "list"]; push: bool}
[@@deriving yojson {strict= false}]

type preference = {list_: bool [@key "list"]; push: bool}
[@@deriving yojson {strict= false}]

type preferences =
  { chat: chat_preference
  ; follow: filterable_preference
  ; like: filterable_preference
  ; like_via_repost: filterable_preference [@key "likeViaRepost"]
  ; mention: filterable_preference
  ; quote: filterable_preference
  ; reply: filterable_preference
  ; repost: filterable_preference
  ; repost_via_repost: filterable_preference [@key "repostViaRepost"]
  ; starterpack_joined: preference [@key "starterpackJoined"]
  ; subscribed_post: preference [@key "subscribedPost"]
  ; unverified: preference
  ; verified: preference }
[@@deriving yojson {strict= false}]

type activity_subscription = {post: bool; reply: bool}
[@@deriving yojson {strict= false}]

type subject_activity_subscription =
  { subject: string
  ; activity_subscription: activity_subscription [@key "activitySubscription"]
  }
[@@deriving yojson {strict= false}]
