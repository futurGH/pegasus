(* generated from com.atproto.repo.defs *)

type commit_meta =
  {
    cid: string;
    rev: string;
  }
[@@deriving yojson {strict= false}]

