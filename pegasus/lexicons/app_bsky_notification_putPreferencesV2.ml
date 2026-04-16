(* generated from app.bsky.notification.putPreferencesV2 *)

(** Set notification-related preferences for an account. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.putPreferencesV2"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      chat: App_bsky_notification_defs.chat_preference option [@default None];
      follow: App_bsky_notification_defs.filterable_preference option [@default None];
      like: App_bsky_notification_defs.filterable_preference option [@default None];
      like_via_repost: App_bsky_notification_defs.filterable_preference option [@key "likeViaRepost"] [@default None];
      mention: App_bsky_notification_defs.filterable_preference option [@default None];
      quote: App_bsky_notification_defs.filterable_preference option [@default None];
      reply: App_bsky_notification_defs.filterable_preference option [@default None];
      repost: App_bsky_notification_defs.filterable_preference option [@default None];
      repost_via_repost: App_bsky_notification_defs.filterable_preference option [@key "repostViaRepost"] [@default None];
      starterpack_joined: App_bsky_notification_defs.preference option [@key "starterpackJoined"] [@default None];
      subscribed_post: App_bsky_notification_defs.preference option [@key "subscribedPost"] [@default None];
      unverified: App_bsky_notification_defs.preference option [@default None];
      verified: App_bsky_notification_defs.preference option [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    preferences: App_bsky_notification_defs.preferences;
  }
[@@deriving yojson {strict= false}]

  let call
      ?chat
      ?follow
      ?like
      ?like_via_repost
      ?mention
      ?quote
      ?reply
      ?repost
      ?repost_via_repost
      ?starterpack_joined
      ?subscribed_post
      ?unverified
      ?verified
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({chat; follow; like; like_via_repost; mention; quote; reply; repost; repost_via_repost; starterpack_joined; subscribed_post; unverified; verified} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

