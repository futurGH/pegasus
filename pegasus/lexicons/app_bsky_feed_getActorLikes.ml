(* generated from app.bsky.feed.getActorLikes *)

(** Get a list of posts liked by an actor. Requires auth, actor must be the requesting account. *)
module Main = struct
  let nsid = "app.bsky.feed.getActorLikes"

  type params =
    { actor: string
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; feed: App_bsky_feed_defs.feed_view_post list }
  [@@deriving yojson {strict= false}]

  let call ~actor ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {actor; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
