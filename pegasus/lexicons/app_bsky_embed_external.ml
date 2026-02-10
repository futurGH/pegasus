(* generated from app.bsky.embed.external *)

type external_ =
  { uri: string
  ; title: string
  ; description: string
  ; thumb: Hermes.blob option [@default None] }
[@@deriving yojson {strict= false}]

type main = {external_: external_ [@key "external"]}
[@@deriving yojson {strict= false}]

type view_external =
  { uri: string
  ; title: string
  ; description: string
  ; thumb: string option [@default None] }
[@@deriving yojson {strict= false}]

type view = {external_: view_external [@key "external"]}
[@@deriving yojson {strict= false}]
