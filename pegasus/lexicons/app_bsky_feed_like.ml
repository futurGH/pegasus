(* generated from app.bsky.feed.like *)

type main =
  { subject: Com_atproto_repo_strongRef.main
  ; created_at: string [@key "createdAt"]
  ; via: Com_atproto_repo_strongRef.main option [@default None] }
[@@deriving yojson {strict= false}]
