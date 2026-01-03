(* generated from app.bsky.graph.getStarterPacksWithMembership *)

type starter_pack_with_membership =
  {
    starter_pack: App_bsky_graph_defs.starter_pack_view [@key "starterPack"];
    list_item: App_bsky_graph_defs.list_item_view option [@key "listItem"] [@default None];
  }
[@@deriving yojson {strict= false}]

(** Enumerates the starter packs created by the session user, and includes membership information about `actor` in those starter packs. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.graph.getStarterPacksWithMembership"

  type params =
  {
    actor: string;
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    starter_packs_with_membership: starter_pack_with_membership list [@key "starterPacksWithMembership"];
  }
[@@deriving yojson {strict= false}]

  let call
      ~actor
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {actor; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

