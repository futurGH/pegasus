(* generated from app.bsky.unspecced.getSuggestedUsers *)

(** Get a list of suggested users *)
module Main = struct
  let nsid = "app.bsky.unspecced.getSuggestedUsers"

  type params =
  {
    category: string option [@default None];
    limit: int option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    actors: App_bsky_actor_defs.profile_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?category
      ?limit
      (client : Hermes.client) : output Lwt.t =
    let params : params = {category; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

