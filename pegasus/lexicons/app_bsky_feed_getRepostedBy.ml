(* generated from app.bsky.feed.getRepostedBy *)

(** Get a list of reposts for a given post. *)
module Main = struct
  let nsid = "app.bsky.feed.getRepostedBy"

  type params =
    { uri: string
    ; cid: string option [@default None]
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { uri: string
    ; cid: string option [@default None]
    ; cursor: string option [@default None]
    ; reposted_by: App_bsky_actor_defs.profile_view list [@key "repostedBy"] }
  [@@deriving yojson {strict= false}]

  let call ~uri ?cid ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {uri; cid; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
