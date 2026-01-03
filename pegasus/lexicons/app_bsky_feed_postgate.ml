(* generated from app.bsky.feed.postgate *)

type disable_rule = unit
let disable_rule_of_yojson _ = Ok ()
let disable_rule_to_yojson () = `Assoc []

type embedding_rules_item =
  | DisableRule of disable_rule
  | Unknown of Yojson.Safe.t

let embedding_rules_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.postgate#disableRule" ->
        (match disable_rule_of_yojson json with
         | Ok v -> Ok (DisableRule v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let embedding_rules_item_to_yojson = function
  | DisableRule v ->
      (match disable_rule_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.postgate#disableRule") :: fields)
       | other -> other)
  | Unknown j -> j

type main =
  {
    created_at: string [@key "createdAt"];
    post: string;
    detached_embedding_uris: string list option [@key "detachedEmbeddingUris"] [@default None];
    embedding_rules: embedding_rules_item list option [@key "embeddingRules"] [@default None];
  }
[@@deriving yojson {strict= false}]

