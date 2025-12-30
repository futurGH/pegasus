(* generated from app.bsky.feed.getFeedGenerators *)

(** Get information about a list of feed generators. *)
module Main = struct
  let nsid = "app.bsky.feed.getFeedGenerators"

  type params =
  {
    feeds: string list;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    feeds: App_bsky_feed_defs.generator_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~feeds
      (client : Hermes.client) : output Lwt.t =
    let params : params = {feeds} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

