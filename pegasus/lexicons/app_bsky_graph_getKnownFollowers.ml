(* generated from app.bsky.graph.getKnownFollowers *)

(** Enumerates accounts which follow a specified account (actor) and are followed by the viewer. *)
module Main = struct
  let nsid = "app.bsky.graph.getKnownFollowers"

  type params =
    { actor: string
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { subject: App_bsky_actor_defs.profile_view
    ; cursor: string option [@default None]
    ; followers: App_bsky_actor_defs.profile_view list }
  [@@deriving yojson {strict= false}]

  let call ~actor ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {actor; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
