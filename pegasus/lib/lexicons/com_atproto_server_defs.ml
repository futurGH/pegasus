(* generated from com.atproto.server.defs *)

type invite_code_use =
  {
    used_by: string [@key "usedBy"];
    used_at: string [@key "usedAt"];
  }
[@@deriving yojson {strict= false}]

type invite_code =
  {
    code: string;
    available: int;
    disabled: bool;
    for_account: string [@key "forAccount"];
    created_by: string [@key "createdBy"];
    created_at: string [@key "createdAt"];
    uses: invite_code_use list;
  }
[@@deriving yojson {strict= false}]

