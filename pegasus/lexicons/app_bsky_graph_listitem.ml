(* generated from app.bsky.graph.listitem *)

type main =
  {
    subject: string;
    list_: string [@key "list"];
    created_at: string [@key "createdAt"];
  }
[@@deriving yojson {strict= false}]

