(* generated from app.bsky.unspecced.getPostThreadV2 *)

type value =
  | ThreadItemPost of App_bsky_unspecced_defs.thread_item_post
  | ThreadItemNoUnauthenticated of App_bsky_unspecced_defs.thread_item_no_unauthenticated
  | ThreadItemNotFound of App_bsky_unspecced_defs.thread_item_not_found
  | ThreadItemBlocked of App_bsky_unspecced_defs.thread_item_blocked
  | Unknown of Yojson.Safe.t

let value_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.unspecced.defs#threadItemPost" ->
        (match App_bsky_unspecced_defs.thread_item_post_of_yojson json with
         | Ok v -> Ok (ThreadItemPost v)
         | Error e -> Error e)
    | "app.bsky.unspecced.defs#threadItemNoUnauthenticated" ->
        (match App_bsky_unspecced_defs.thread_item_no_unauthenticated_of_yojson json with
         | Ok v -> Ok (ThreadItemNoUnauthenticated v)
         | Error e -> Error e)
    | "app.bsky.unspecced.defs#threadItemNotFound" ->
        (match App_bsky_unspecced_defs.thread_item_not_found_of_yojson json with
         | Ok v -> Ok (ThreadItemNotFound v)
         | Error e -> Error e)
    | "app.bsky.unspecced.defs#threadItemBlocked" ->
        (match App_bsky_unspecced_defs.thread_item_blocked_of_yojson json with
         | Ok v -> Ok (ThreadItemBlocked v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let value_to_yojson = function
  | ThreadItemPost v ->
      (match App_bsky_unspecced_defs.thread_item_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.unspecced.defs#threadItemPost") :: fields)
       | other -> other)
  | ThreadItemNoUnauthenticated v ->
      (match App_bsky_unspecced_defs.thread_item_no_unauthenticated_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.unspecced.defs#threadItemNoUnauthenticated") :: fields)
       | other -> other)
  | ThreadItemNotFound v ->
      (match App_bsky_unspecced_defs.thread_item_not_found_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.unspecced.defs#threadItemNotFound") :: fields)
       | other -> other)
  | ThreadItemBlocked v ->
      (match App_bsky_unspecced_defs.thread_item_blocked_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.unspecced.defs#threadItemBlocked") :: fields)
       | other -> other)
  | Unknown j -> j

type thread_item =
  {
    uri: string;
    depth: int;
    value: value;
  }
[@@deriving yojson {strict= false}]

(** (NOTE: this endpoint is under development and WILL change without notice. Don't use it until it is moved out of `unspecced` or your application WILL break) Get posts in a thread. It is based in an anchor post at any depth of the tree, and returns posts above it (recursively resolving the parent, without further branching to their replies) and below it (recursive replies, with branching to their replies). Does not require auth, but additional metadata and filtering will be applied for authed requests. *)
module Main = struct
  let nsid = "app.bsky.unspecced.getPostThreadV2"

  type params =
  {
    anchor: string;
    above: bool option [@default None];
    below: int option [@default None];
    branching_factor: int option [@key "branchingFactor"] [@default None];
    sort: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    thread: thread_item list;
    threadgate: App_bsky_feed_defs.threadgate_view option [@default None];
    has_other_replies: bool [@key "hasOtherReplies"];
  }
[@@deriving yojson {strict= false}]

  let call
      ~anchor
      ?above
      ?below
      ?branching_factor
      ?sort
      (client : Hermes.client) : output Lwt.t =
    let params : params = {anchor; above; below; branching_factor; sort} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

