(* generated from app.bsky.feed.getFeed *)

(** Get a hydrated feed from an actor's selected feed generator. Implemented by App View. *)
module Main = struct
  let nsid = "app.bsky.feed.getFeed"

  type params =
  {
    feed: string;
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
      ~feed
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {feed; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

