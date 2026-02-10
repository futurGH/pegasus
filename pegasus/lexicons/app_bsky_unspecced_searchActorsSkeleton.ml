(* generated from app.bsky.unspecced.searchActorsSkeleton *)

(** Backend Actors (profile) search, returns only skeleton. *)
module Main = struct
  let nsid = "app.bsky.unspecced.searchActorsSkeleton"

  type params =
    { q: string
    ; viewer: string option [@default None]
    ; typeahead: bool option [@default None]
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; hits_total: int option [@key "hitsTotal"] [@default None]
    ; actors: App_bsky_unspecced_defs.skeleton_search_actor list }
  [@@deriving yojson {strict= false}]

  let call ~q ?viewer ?typeahead ?limit ?cursor (client : Hermes.client) :
      output Lwt.t =
    let params : params = {q; viewer; typeahead; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
