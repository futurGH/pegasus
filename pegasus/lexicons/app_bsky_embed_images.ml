(* generated from app.bsky.embed.images *)

type image =
  { image: Hermes.blob
  ; alt: string
  ; aspect_ratio: App_bsky_embed_defs.aspect_ratio option
        [@key "aspectRatio"] [@default None] }
[@@deriving yojson {strict= false}]

type main = {images: image list} [@@deriving yojson {strict= false}]

type view_image =
  { thumb: string
  ; fullsize: string
  ; alt: string
  ; aspect_ratio: App_bsky_embed_defs.aspect_ratio option
        [@key "aspectRatio"] [@default None] }
[@@deriving yojson {strict= false}]

type view = {images: view_image list} [@@deriving yojson {strict= false}]
