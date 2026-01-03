(* generated from app.bsky.feed.getAuthorFeed *)

(** Get a view of an actor's 'author feed' (post and reposts by the author). Does not require auth. *)
module Main = struct
  let nsid = "app.bsky.feed.getAuthorFeed"

  type params =
  {
    actor: string;
    limit: int option [@default None];
    cursor: string option [@default None];
    filter: string option [@default None];
    include_pins: bool option [@key "includePins"] [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    feed: App_bsky_feed_defs.feed_view_post list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~actor
      ?limit
      ?cursor
      ?filter
      ?include_pins
      (client : Hermes.client) : output Lwt.t =
    let params : params = {actor; limit; cursor; filter; include_pins} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

