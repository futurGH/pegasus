(* generated from app.bsky.graph.getStarterPacks *)

(** Get views for a list of starter packs. *)
module Main = struct
  let nsid = "app.bsky.graph.getStarterPacks"

  type params =
  {
    uris: string list;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    starter_packs: App_bsky_graph_defs.starter_pack_view_basic list [@key "starterPacks"];
  }
[@@deriving yojson {strict= false}]

  let call
      ~uris
      (client : Hermes.client) : output Lwt.t =
    let params : params = {uris} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

