(* generated from app.bsky.graph.searchStarterPacks *)

(** Find starter packs matching search criteria. Does not require auth. *)
module Main = struct
  let nsid = "app.bsky.graph.searchStarterPacks"

  type params =
    { q: string
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; starter_packs: App_bsky_graph_defs.starter_pack_view_basic list
          [@key "starterPacks"] }
  [@@deriving yojson {strict= false}]

  let call ~q ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {q; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
