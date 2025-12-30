(* generated from app.bsky.graph.follow *)

type main =
  {
    subject: string;
    created_at: string [@key "createdAt"];
    via: Com_atproto_repo_strongRef.main option [@default None];
  }
[@@deriving yojson {strict= false}]

