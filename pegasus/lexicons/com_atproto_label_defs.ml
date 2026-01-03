(* generated from com.atproto.label.defs *)

type label =
  {
    ver: int option [@default None];
    src: string;
    uri: string;
    cid: string option [@default None];
    val_: string [@key "val"];
    neg: bool option [@default None];
    cts: string;
    exp: string option [@default None];
    sig_: bytes option [@key "sig"] [@default None];
  }
[@@deriving yojson {strict= false}]

type self_label =
  {
    val_: string [@key "val"];
  }
[@@deriving yojson {strict= false}]

type self_labels =
  {
    values: self_label list;
  }
[@@deriving yojson {strict= false}]

type label_value_definition_strings =
  {
    lang: string;
    name: string;
    description: string;
  }
[@@deriving yojson {strict= false}]

type label_value_definition =
  {
    identifier: string;
    severity: string;
    blurs: string;
    default_setting: string option [@key "defaultSetting"] [@default None];
    adult_only: bool option [@key "adultOnly"] [@default None];
    locales: label_value_definition_strings list;
  }
[@@deriving yojson {strict= false}]

(** string type with known values *)
type label_value = string
let label_value_of_yojson = function
  | `String s -> Ok s
  | _ -> Error "label_value: expected string"
let label_value_to_yojson s = `String s

