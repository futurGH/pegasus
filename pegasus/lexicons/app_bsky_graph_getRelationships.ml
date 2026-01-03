(* generated from app.bsky.graph.getRelationships *)

(** Enumerates public relationships between one account, and a list of other accounts. Does not require auth. *)
module Main = struct
  let nsid = "app.bsky.graph.getRelationships"

  type params =
  {
    actor: string;
    others: string list option [@default None] [@of_yojson Hermes_util.query_string_list_option_of_yojson] [@to_yojson Hermes_util.query_string_list_option_to_yojson];
  }
[@@deriving yojson {strict= false}]

  type relationships_item =
  | Relationship of App_bsky_graph_defs.relationship
  | NotFoundActor of App_bsky_graph_defs.not_found_actor
  | Unknown of Yojson.Safe.t

let relationships_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.graph.defs#relationship" ->
        (match App_bsky_graph_defs.relationship_of_yojson json with
         | Ok v -> Ok (Relationship v)
         | Error e -> Error e)
    | "app.bsky.graph.defs#notFoundActor" ->
        (match App_bsky_graph_defs.not_found_actor_of_yojson json with
         | Ok v -> Ok (NotFoundActor v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let relationships_item_to_yojson = function
  | Relationship v ->
      (match App_bsky_graph_defs.relationship_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.graph.defs#relationship") :: fields)
       | other -> other)
  | NotFoundActor v ->
      (match App_bsky_graph_defs.not_found_actor_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.graph.defs#notFoundActor") :: fields)
       | other -> other)
  | Unknown j -> j

type output =
  {
    actor: string option [@default None];
    relationships: relationships_item list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~actor
      ?others
      (client : Hermes.client) : output Lwt.t =
    let params : params = {actor; others} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

