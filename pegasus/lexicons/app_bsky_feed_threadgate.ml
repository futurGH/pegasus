(* generated from app.bsky.feed.threadgate *)

type list_rule = {list_: string [@key "list"]}
[@@deriving yojson {strict= false}]

type following_rule = unit

let following_rule_of_yojson _ = Ok ()

let following_rule_to_yojson () = `Assoc []

type follower_rule = unit

let follower_rule_of_yojson _ = Ok ()

let follower_rule_to_yojson () = `Assoc []

type mention_rule = unit

let mention_rule_of_yojson _ = Ok ()

let mention_rule_to_yojson () = `Assoc []

type allow_item =
  | MentionRule of mention_rule
  | FollowerRule of follower_rule
  | FollowingRule of following_rule
  | ListRule of list_rule
  | Unknown of Yojson.Safe.t

let allow_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.threadgate#mentionRule" -> (
      match mention_rule_of_yojson json with
      | Ok v ->
          Ok (MentionRule v)
      | Error e ->
          Error e )
    | "app.bsky.feed.threadgate#followerRule" -> (
      match follower_rule_of_yojson json with
      | Ok v ->
          Ok (FollowerRule v)
      | Error e ->
          Error e )
    | "app.bsky.feed.threadgate#followingRule" -> (
      match following_rule_of_yojson json with
      | Ok v ->
          Ok (FollowingRule v)
      | Error e ->
          Error e )
    | "app.bsky.feed.threadgate#listRule" -> (
      match list_rule_of_yojson json with
      | Ok v ->
          Ok (ListRule v)
      | Error e ->
          Error e )
    | _ ->
        Ok (Unknown json)
  with _ -> Error "failed to parse union"

let allow_item_to_yojson = function
  | MentionRule v -> (
    match mention_rule_to_yojson v with
    | `Assoc fields ->
        `Assoc
          (("$type", `String "app.bsky.feed.threadgate#mentionRule") :: fields)
    | other ->
        other )
  | FollowerRule v -> (
    match follower_rule_to_yojson v with
    | `Assoc fields ->
        `Assoc
          (("$type", `String "app.bsky.feed.threadgate#followerRule") :: fields)
    | other ->
        other )
  | FollowingRule v -> (
    match following_rule_to_yojson v with
    | `Assoc fields ->
        `Assoc
          (("$type", `String "app.bsky.feed.threadgate#followingRule") :: fields)
    | other ->
        other )
  | ListRule v -> (
    match list_rule_to_yojson v with
    | `Assoc fields ->
        `Assoc (("$type", `String "app.bsky.feed.threadgate#listRule") :: fields)
    | other ->
        other )
  | Unknown j ->
      j

type main =
  { post: string
  ; allow: allow_item list option [@default None]
  ; created_at: string [@key "createdAt"]
  ; hidden_replies: string list option [@key "hiddenReplies"] [@default None] }
[@@deriving yojson {strict= false}]
