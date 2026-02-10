(* generated from app.bsky.unspecced.getSuggestionsSkeleton *)

(** Get a skeleton of suggested actors. Intended to be called and then hydrated through app.bsky.actor.getSuggestions *)
module Main = struct
  let nsid = "app.bsky.unspecced.getSuggestionsSkeleton"

  type params =
    { viewer: string option [@default None]
    ; limit: int option [@default None]
    ; cursor: string option [@default None]
    ; relative_to_did: string option [@key "relativeToDid"] [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; actors: App_bsky_unspecced_defs.skeleton_search_actor list
    ; relative_to_did: string option [@key "relativeToDid"] [@default None]
    ; rec_id: int option [@key "recId"] [@default None] }
  [@@deriving yojson {strict= false}]

  let call ?viewer ?limit ?cursor ?relative_to_did (client : Hermes.client) :
      output Lwt.t =
    let params : params = {viewer; limit; cursor; relative_to_did} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
