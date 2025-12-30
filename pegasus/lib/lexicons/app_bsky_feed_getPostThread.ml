(* generated from app.bsky.feed.getPostThread *)

(** Get posts in a thread. Does not require auth, but additional metadata and filtering will be applied for authed requests. *)
module Main = struct
  let nsid = "app.bsky.feed.getPostThread"

  type params =
  {
    uri: string;
    depth: int option [@default None];
    parent_height: int option [@key "parentHeight"] [@default None];
  }
[@@deriving yojson {strict= false}]

  type thread =
  | ThreadViewPost of App_bsky_feed_defs.thread_view_post
  | NotFoundPost of App_bsky_feed_defs.not_found_post
  | BlockedPost of App_bsky_feed_defs.blocked_post
  | Unknown of Yojson.Safe.t

let thread_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.feed.defs#threadViewPost" ->
        (match App_bsky_feed_defs.thread_view_post_of_yojson json with
         | Ok v -> Ok (ThreadViewPost v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#notFoundPost" ->
        (match App_bsky_feed_defs.not_found_post_of_yojson json with
         | Ok v -> Ok (NotFoundPost v)
         | Error e -> Error e)
    | "app.bsky.feed.defs#blockedPost" ->
        (match App_bsky_feed_defs.blocked_post_of_yojson json with
         | Ok v -> Ok (BlockedPost v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let thread_to_yojson = function
  | ThreadViewPost v ->
      (match App_bsky_feed_defs.thread_view_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#threadViewPost") :: fields)
       | other -> other)
  | NotFoundPost v ->
      (match App_bsky_feed_defs.not_found_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#notFoundPost") :: fields)
       | other -> other)
  | BlockedPost v ->
      (match App_bsky_feed_defs.blocked_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.feed.defs#blockedPost") :: fields)
       | other -> other)
  | Unknown j -> j

type output =
  {
    thread: thread;
    threadgate: App_bsky_feed_defs.threadgate_view option [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ~uri
      ?depth
      ?parent_height
      (client : Hermes.client) : output Lwt.t =
    let params : params = {uri; depth; parent_height} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

