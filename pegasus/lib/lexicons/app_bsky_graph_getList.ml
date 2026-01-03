(* generated from app.bsky.graph.getList *)

(** Gets a 'view' (with additional context) of a specified list. *)
module Main = struct
  let nsid = "app.bsky.graph.getList"

  type params =
  {
    list_: string [@key "list"];
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    list_: App_bsky_graph_defs.list_view [@key "list"];
    items: App_bsky_graph_defs.list_item_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~list_
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {list_; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

