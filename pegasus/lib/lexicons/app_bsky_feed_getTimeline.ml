(* generated from app.bsky.feed.getTimeline *)

(** Get a view of the requesting account's home timeline. This is expected to be some form of reverse-chronological feed. *)
module Main = struct
  let nsid = "app.bsky.feed.getTimeline"

  type params =
  {
    algorithm: string option [@default None];
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    feed: App_bsky_feed_defs.feed_view_post list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?algorithm
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {algorithm; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

