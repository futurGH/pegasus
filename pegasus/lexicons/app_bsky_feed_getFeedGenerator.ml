(* generated from app.bsky.feed.getFeedGenerator *)

(** Get information about a feed generator. Implemented by AppView. *)
module Main = struct
  let nsid = "app.bsky.feed.getFeedGenerator"

  type params = {feed: string} [@@deriving yojson {strict= false}]

  type output =
    { view: App_bsky_feed_defs.generator_view
    ; is_online: bool [@key "isOnline"]
    ; is_valid: bool [@key "isValid"] }
  [@@deriving yojson {strict= false}]

  let call ~feed (client : Hermes.client) : output Lwt.t =
    let params : params = {feed} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
