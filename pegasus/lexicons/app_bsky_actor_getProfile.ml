(* generated from app.bsky.actor.getProfile *)

(** Get detailed profile view of an actor. Does not require auth, but contains relevant metadata with auth. *)
module Main = struct
  let nsid = "app.bsky.actor.getProfile"

  type params = {actor: string} [@@deriving yojson {strict= false}]

  type output = App_bsky_actor_defs.profile_view_detailed
  [@@deriving yojson {strict= false}]

  let call ~actor (client : Hermes.client) : output Lwt.t =
    let params : params = {actor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
