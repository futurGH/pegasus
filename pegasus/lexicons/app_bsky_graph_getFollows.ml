(* generated from app.bsky.graph.getFollows *)

(** Enumerates accounts which a specified account (actor) follows. *)
module Main = struct
  let nsid = "app.bsky.graph.getFollows"

  type params =
    { actor: string
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { subject: App_bsky_actor_defs.profile_view
    ; cursor: string option [@default None]
    ; follows: App_bsky_actor_defs.profile_view list }
  [@@deriving yojson {strict= false}]

  let call ~actor ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {actor; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
