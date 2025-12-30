(* generated from app.bsky.graph.listitem *)

type main =
  {
    subject: string;
    list: string;
    created_at: string [@key "createdAt"];
  }
[@@deriving yojson {strict= false}]

