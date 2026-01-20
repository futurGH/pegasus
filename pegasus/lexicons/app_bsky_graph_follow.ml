(* generated from app.bsky.graph.follow *)

type main =
  {
    subject: string;
    created_at: string [@key "createdAt"];
  }
[@@deriving yojson {strict= false}]

