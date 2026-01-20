(* generated from app.bsky.bookmark.defs *)

type bookmark = {subject: Com_atproto_repo_strongRef.main}
[@@deriving yojson {strict= false}]

type item =
  | BlockedPost of App_bsky_feed_defs.blocked_post
  | NotFoundPost of App_bsky_feed_defs.not_found_post
  | PostView of App_bsky_feed_defs.post_view
  | Unknown of Yojson.Safe.t

let item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.defs#blockedPost" -> (
      match App_bsky_feed_defs.blocked_post_of_yojson json with
      | Ok v ->
          Ok (BlockedPost v)
      | Error e ->
          Error e )
    | "app.bsky.feed.defs#notFoundPost" -> (
      match App_bsky_feed_defs.not_found_post_of_yojson json with
      | Ok v ->
          Ok (NotFoundPost v)
      | Error e ->
          Error e )
    | "app.bsky.feed.defs#postView" -> (
      match App_bsky_feed_defs.post_view_of_yojson json with
      | Ok v ->
          Ok (PostView v)
      | Error e ->
          Error e )
    | _ ->
        Ok (Unknown json)
  with _ -> Error "failed to parse union"

let item_to_yojson = function
  | BlockedPost v -> (
    match App_bsky_feed_defs.blocked_post_to_yojson v with
    | `Assoc fields ->
        `Assoc (("$type", `String "app.bsky.feed.defs#blockedPost") :: fields)
    | other ->
        other )
  | NotFoundPost v -> (
    match App_bsky_feed_defs.not_found_post_to_yojson v with
    | `Assoc fields ->
        `Assoc (("$type", `String "app.bsky.feed.defs#notFoundPost") :: fields)
    | other ->
        other )
  | PostView v -> (
    match App_bsky_feed_defs.post_view_to_yojson v with
    | `Assoc fields ->
        `Assoc (("$type", `String "app.bsky.feed.defs#postView") :: fields)
    | other ->
        other )
  | Unknown j ->
      j

type bookmark_view =
  { subject: Com_atproto_repo_strongRef.main
  ; created_at: string option [@key "createdAt"] [@default None]
  ; item: item }
[@@deriving yojson {strict= false}]
