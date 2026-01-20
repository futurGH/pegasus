(* generated from app.bsky.unspecced.getOnboardingSuggestedStarterPacks *)

(** Get a list of suggested starterpacks for onboarding *)
module Main = struct
  let nsid = "app.bsky.unspecced.getOnboardingSuggestedStarterPacks"

  type params = {limit: int option [@default None]}
  [@@deriving yojson {strict= false}]

  type output =
    { starter_packs: App_bsky_graph_defs.starter_pack_view list
          [@key "starterPacks"] }
  [@@deriving yojson {strict= false}]

  let call ?limit (client : Hermes.client) : output Lwt.t =
    let params : params = {limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
