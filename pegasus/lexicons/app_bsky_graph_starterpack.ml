(* generated from app.bsky.graph.starterpack *)

type feed_item = {uri: string} [@@deriving yojson {strict= false}]

type main =
  { name: string
  ; description: string option [@default None]
  ; description_facets: App_bsky_richtext_facet.main list option
        [@key "descriptionFacets"] [@default None]
  ; list_: string [@key "list"]
  ; feeds: feed_item list option [@default None]
  ; created_at: string [@key "createdAt"] }
[@@deriving yojson {strict= false}]
