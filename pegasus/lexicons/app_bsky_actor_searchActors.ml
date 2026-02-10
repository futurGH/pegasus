(* generated from app.bsky.actor.searchActors *)

(** Find actors (profiles) matching search criteria. Does not require auth. *)
module Main = struct
  let nsid = "app.bsky.actor.searchActors"

  type params =
    { term: string option [@default None]
    ; q: string option [@default None]
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; actors: App_bsky_actor_defs.profile_view list }
  [@@deriving yojson {strict= false}]

  let call ?term ?q ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {term; q; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
