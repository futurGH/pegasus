(* generated from app.bsky.unspecced.getSuggestedUsersForExploreSkeleton *)

(** Get a skeleton of suggested users for the Explore page. Intended to be called and hydrated by app.bsky.unspecced.getSuggestedUsersForExplore *)
module Main = struct
  let nsid = "app.bsky.unspecced.getSuggestedUsersForExploreSkeleton"

  type params =
  {
    viewer: string option [@default None];
    category: string option [@default None];
    limit: int option [@default None];
  }
[@@xrpc_query]

  type output =
  {
    dids: string list;
    rec_id_str: string option [@key "recIdStr"] [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ?viewer
      ?category
      ?limit
      (client : Hermes.client) : output Lwt.t =
    let params : params = {viewer; category; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

