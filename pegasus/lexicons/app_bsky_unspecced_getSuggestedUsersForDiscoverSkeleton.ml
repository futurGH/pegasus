(* generated from app.bsky.unspecced.getSuggestedUsersForDiscoverSkeleton *)

(** Get a skeleton of suggested users for the Discover page. Intended to be called and hydrated by app.bsky.unspecced.getSuggestedUsersForDiscover *)
module Main = struct
  let nsid = "app.bsky.unspecced.getSuggestedUsersForDiscoverSkeleton"

  type params =
  {
    viewer: string option [@default None];
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
      ?limit
      (client : Hermes.client) : output Lwt.t =
    let params : params = {viewer; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

