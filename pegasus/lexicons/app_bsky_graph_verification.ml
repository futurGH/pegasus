(* generated from app.bsky.graph.verification *)

type main =
  {
    subject: string;
    handle: string;
    display_name: string [@key "displayName"];
    created_at: string [@key "createdAt"];
  }
[@@deriving yojson {strict= false}]

