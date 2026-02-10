(* generated from app.bsky.embed.video *)

type caption = {lang: string; file: Hermes.blob}
[@@deriving yojson {strict= false}]

type main =
  { video: Hermes.blob
  ; captions: caption list option [@default None]
  ; alt: string option [@default None]
  ; aspect_ratio: App_bsky_embed_defs.aspect_ratio option
        [@key "aspectRatio"] [@default None] }
[@@deriving yojson {strict= false}]

type view =
  { cid: string
  ; playlist: string
  ; thumbnail: string option [@default None]
  ; alt: string option [@default None]
  ; aspect_ratio: App_bsky_embed_defs.aspect_ratio option
        [@key "aspectRatio"] [@default None] }
[@@deriving yojson {strict= false}]
