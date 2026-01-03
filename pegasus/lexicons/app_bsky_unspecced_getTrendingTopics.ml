(* generated from app.bsky.unspecced.getTrendingTopics *)

(** Get a list of trending topics *)
module Main = struct
  let nsid = "app.bsky.unspecced.getTrendingTopics"

  type params =
  {
    viewer: string option [@default None];
    limit: int option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    topics: App_bsky_unspecced_defs.trending_topic list;
    suggested: App_bsky_unspecced_defs.trending_topic list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?viewer
      ?limit
      (client : Hermes.client) : output Lwt.t =
    let params : params = {viewer; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

