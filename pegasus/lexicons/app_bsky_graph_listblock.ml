(* generated from app.bsky.graph.listblock *)

type main =
  {
    subject: string;
    created_at: string [@key "createdAt"];
  }
[@@deriving yojson {strict= false}]

