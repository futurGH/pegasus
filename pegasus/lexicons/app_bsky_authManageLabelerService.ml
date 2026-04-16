(* generated from app.bsky.authManageLabelerService *)

(** app.bsky.authManageLabelerService *)
type permission =
  { resource: string
  ; lxm: string list option [@default None]
  ; aud: string option [@default None]
  ; inherit_aud: bool option [@key "inheritAud"] [@default None]
  ; collection: string list option [@default None]
  ; action: string list option [@default None]
  ; accept: string list option [@default None] }
[@@deriving yojson {strict= false}]

type main =
  { title: string option [@default None]
  ; detail: string option [@default None]
  ; permissions: permission list }
[@@deriving yojson {strict= false}]

