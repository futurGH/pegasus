(* generated from app.bsky.graph.getSuggestedFollowsByActor *)

(** Enumerates follows similar to a given account (actor). Expected use is to recommend additional accounts immediately after following one account. *)
module Main = struct
  let nsid = "app.bsky.graph.getSuggestedFollowsByActor"

  type params =
  {
    actor: string;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    suggestions: App_bsky_actor_defs.profile_view list;
    is_fallback: bool option [@key "isFallback"] [@default None];
    rec_id: int option [@key "recId"] [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ~actor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {actor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

