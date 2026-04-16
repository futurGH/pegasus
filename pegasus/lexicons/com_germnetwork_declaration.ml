(* generated from com.germnetwork.declaration *)

type message_me =
  {
    message_me_url: string [@key "messageMeUrl"];
    show_button_to: string [@key "showButtonTo"];
  }
[@@deriving yojson {strict= false}]

type main =
  {
    version: string;
    current_key: bytes [@key "currentKey"];
    message_me: message_me option [@key "messageMe"] [@default None];
    key_package: bytes option [@key "keyPackage"] [@default None];
    continuity_proofs: bytes list option [@key "continuityProofs"] [@default None];
  }
[@@deriving yojson {strict= false}]

