(* generated from com.atproto.identity.defs *)

type identity_info =
  {did: string; handle: string; did_doc: Yojson.Safe.t [@key "didDoc"]}
[@@deriving yojson {strict= false}]
