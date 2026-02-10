(* generated from app.bsky.graph.block *)

type main = {subject: string; created_at: string [@key "createdAt"]}
[@@deriving yojson {strict= false}]
