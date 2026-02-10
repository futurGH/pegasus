(* generated from app.bsky.unspecced.searchStarterPacksSkeleton *)

(** Backend Starter Pack search, returns only skeleton. *)
module Main = struct
  let nsid = "app.bsky.unspecced.searchStarterPacksSkeleton"

  type params =
    { q: string
    ; viewer: string option [@default None]
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; hits_total: int option [@key "hitsTotal"] [@default None]
    ; starter_packs: App_bsky_unspecced_defs.skeleton_search_starter_pack list
          [@key "starterPacks"] }
  [@@deriving yojson {strict= false}]

  let call ~q ?viewer ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {q; viewer; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
