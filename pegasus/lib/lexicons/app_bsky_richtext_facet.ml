(* generated from app.bsky.richtext.facet *)

type tag =
  {
    tag: string;
  }
[@@deriving yojson {strict= false}]

type link =
  {
    uri: string;
  }
[@@deriving yojson {strict= false}]

type mention =
  {
    did: string;
  }
[@@deriving yojson {strict= false}]

type byte_slice =
  {
    byte_start: int [@key "byteStart"];
    byte_end: int [@key "byteEnd"];
  }
[@@deriving yojson {strict= false}]

type features_item =
  | Mention of mention
  | Link of link
  | Tag of tag
  | Unknown of Yojson.Safe.t

let features_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.richtext.facet#mention" ->
        (match mention_of_yojson json with
         | Ok v -> Ok (Mention v)
         | Error e -> Error e)
    | "app.bsky.richtext.facet#link" ->
        (match link_of_yojson json with
         | Ok v -> Ok (Link v)
         | Error e -> Error e)
    | "app.bsky.richtext.facet#tag" ->
        (match tag_of_yojson json with
         | Ok v -> Ok (Tag v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let features_item_to_yojson = function
  | Mention v ->
      (match mention_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.richtext.facet#mention") :: fields)
       | other -> other)
  | Link v ->
      (match link_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.richtext.facet#link") :: fields)
       | other -> other)
  | Tag v ->
      (match tag_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.richtext.facet#tag") :: fields)
       | other -> other)
  | Unknown j -> j

type main =
  {
    index: byte_slice;
    features: features_item list;
  }
[@@deriving yojson {strict= false}]

