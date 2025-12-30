(* generated from app.bsky.actor.getProfiles *)

(** Get detailed profile views of multiple actors. *)
module Main = struct
  let nsid = "app.bsky.actor.getProfiles"

  type params =
  {
    actors: string list;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    profiles: App_bsky_actor_defs.profile_view_detailed list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~actors
      (client : Hermes.client) : output Lwt.t =
    let params : params = {actors} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

