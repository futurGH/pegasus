(* generated from app.bsky.graph.getStarterPack *)

(** Gets a view of a starter pack. *)
module Main = struct
  let nsid = "app.bsky.graph.getStarterPack"

  type params = {starter_pack: string [@key "starterPack"]}
  [@@deriving yojson {strict= false}]

  type output =
    {starter_pack: App_bsky_graph_defs.starter_pack_view [@key "starterPack"]}
  [@@deriving yojson {strict= false}]

  let call ~starter_pack (client : Hermes.client) : output Lwt.t =
    let params : params = {starter_pack} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
