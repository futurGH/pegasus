(* generated from app.bsky.graph.getLists *)

(** Enumerates the lists created by a specified account (actor). *)
module Main = struct
  let nsid = "app.bsky.graph.getLists"

  type params =
  {
    actor: string;
    limit: int option [@default None];
    cursor: string option [@default None];
    purposes: string list option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    lists: App_bsky_graph_defs.list_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~actor
      ?limit
      ?cursor
      ?purposes
      (client : Hermes.client) : output Lwt.t =
    let params : params = {actor; limit; cursor; purposes} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

