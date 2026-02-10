(* generated from app.bsky.graph.getListsWithMembership *)

type list_with_membership =
  { list_: App_bsky_graph_defs.list_view [@key "list"]
  ; list_item: App_bsky_graph_defs.list_item_view option
        [@key "listItem"] [@default None] }
[@@deriving yojson {strict= false}]

(** Enumerates the lists created by the session user, and includes membership information about `actor` in those lists. Only supports curation and moderation lists (no reference lists, used in starter packs). Requires auth. *)
module Main = struct
  let nsid = "app.bsky.graph.getListsWithMembership"

  type params =
    { actor: string
    ; limit: int option [@default None]
    ; cursor: string option [@default None]
    ; purposes: string list option
          [@default None]
          [@of_yojson Hermes_util.query_string_list_option_of_yojson]
          [@to_yojson Hermes_util.query_string_list_option_to_yojson] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; lists_with_membership: list_with_membership list
          [@key "listsWithMembership"] }
  [@@deriving yojson {strict= false}]

  let call ~actor ?limit ?cursor ?purposes (client : Hermes.client) :
      output Lwt.t =
    let params : params = {actor; limit; cursor; purposes} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
