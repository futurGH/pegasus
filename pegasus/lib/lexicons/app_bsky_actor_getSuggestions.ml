(* generated from app.bsky.actor.getSuggestions *)

(** Get a list of suggested actors. Expected use is discovery of accounts to follow during new account onboarding. *)
module Main = struct
  let nsid = "app.bsky.actor.getSuggestions"

  type params =
  {
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    actors: App_bsky_actor_defs.profile_view list;
    rec_id: int option [@key "recId"] [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

