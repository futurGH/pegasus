(* generated from app.bsky.actor.searchActorsTypeahead *)

(** Find actor suggestions for a prefix search term. Expected use is for auto-completion during text field entry. Does not require auth. *)
module Main = struct
  let nsid = "app.bsky.actor.searchActorsTypeahead"

  type params =
    { term: string option [@default None]
    ; q: string option [@default None]
    ; limit: int option [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {actors: App_bsky_actor_defs.profile_view_basic list}
  [@@deriving yojson {strict= false}]

  let call ?term ?q ?limit (client : Hermes.client) : output Lwt.t =
    let params : params = {term; q; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
