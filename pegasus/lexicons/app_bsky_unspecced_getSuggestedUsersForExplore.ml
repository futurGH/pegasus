(* generated from app.bsky.unspecced.getSuggestedUsersForExplore *)

(** Get a list of suggested users for the Explore page *)
module Main = struct
  let nsid = "app.bsky.unspecced.getSuggestedUsersForExplore"

  type params =
  {
    category: string option [@default None];
    limit: int option [@default None];
  }
[@@xrpc_query]

  type output =
  {
    actors: App_bsky_actor_defs.profile_view list;
    rec_id_str: string option [@key "recIdStr"] [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ?category
      ?limit
      (client : Hermes.client) : output Lwt.t =
    let params : params = {category; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

