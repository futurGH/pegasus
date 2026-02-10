(* generated from app.bsky.unspecced.getSuggestedFeeds *)

(** Get a list of suggested feeds *)
module Main = struct
  let nsid = "app.bsky.unspecced.getSuggestedFeeds"

  type params = {limit: int option [@default None]}
  [@@deriving yojson {strict= false}]

  type output = {feeds: App_bsky_feed_defs.generator_view list}
  [@@deriving yojson {strict= false}]

  let call ?limit (client : Hermes.client) : output Lwt.t =
    let params : params = {limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
