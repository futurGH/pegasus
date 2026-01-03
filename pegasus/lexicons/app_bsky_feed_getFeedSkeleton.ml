(* generated from app.bsky.feed.getFeedSkeleton *)

(** Get a skeleton of a feed provided by a feed generator. Auth is optional, depending on provider requirements, and provides the DID of the requester. Implemented by Feed Generator Service. *)
module Main = struct
  let nsid = "app.bsky.feed.getFeedSkeleton"

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
    feed: App_bsky_feed_defs.skeleton_feed_post list;
    req_id: string option [@key "reqId"] [@default None];
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

