(* generated from app.bsky.unspecced.getSuggestedFeedsSkeleton *)

(** Get a skeleton of suggested feeds. Intended to be called and hydrated by app.bsky.unspecced.getSuggestedFeeds *)
module Main = struct
  let nsid = "app.bsky.unspecced.getSuggestedFeedsSkeleton"

  type params =
    {viewer: string option [@default None]; limit: int option [@default None]}
  [@@deriving yojson {strict= false}]

  type output = {feeds: string list} [@@deriving yojson {strict= false}]

  let call ?viewer ?limit (client : Hermes.client) : output Lwt.t =
    let params : params = {viewer; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
