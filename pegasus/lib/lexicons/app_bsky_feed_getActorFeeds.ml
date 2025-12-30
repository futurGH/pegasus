(* generated from app.bsky.feed.getActorFeeds *)

(** Get a list of feeds (feed generator records) created by the actor (in the actor's repo). *)
module Main = struct
  let nsid = "app.bsky.feed.getActorFeeds"

  type params =
  {
    actor: string;
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    feeds: App_bsky_feed_defs.generator_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~actor
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {actor; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

