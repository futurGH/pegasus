(* generated from app.bsky.feed.getListFeed *)

(** Get a feed of recent posts from a list (posts and reposts from any actors on the list). Does not require auth. *)
module Main = struct
  let nsid = "app.bsky.feed.getListFeed"

  type params =
  {
    list: string;
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
      ~list
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {list; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

