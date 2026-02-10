(* generated from app.bsky.feed.getSuggestedFeeds *)

(** Get a list of suggested feeds (feed generators) for the requesting account. *)
module Main = struct
  let nsid = "app.bsky.feed.getSuggestedFeeds"

  type params =
    {limit: int option [@default None]; cursor: string option [@default None]}
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; feeds: App_bsky_feed_defs.generator_view list }
  [@@deriving yojson {strict= false}]

  let call ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
