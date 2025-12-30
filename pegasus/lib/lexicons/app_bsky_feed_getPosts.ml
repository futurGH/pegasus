(* generated from app.bsky.feed.getPosts *)

(** Gets post views for a specified list of posts (by AT-URI). This is sometimes referred to as 'hydrating' a 'feed skeleton'. *)
module Main = struct
  let nsid = "app.bsky.feed.getPosts"

  type params =
  {
    uris: string list;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    posts: App_bsky_feed_defs.post_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~uris
      (client : Hermes.client) : output Lwt.t =
    let params : params = {uris} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

