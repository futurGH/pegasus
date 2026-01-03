(* generated from app.bsky.graph.getListMutes *)

(** Enumerates mod lists that the requesting account (actor) currently has muted. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.graph.getListMutes"

  type params =
  {
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    lists: App_bsky_graph_defs.list_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

