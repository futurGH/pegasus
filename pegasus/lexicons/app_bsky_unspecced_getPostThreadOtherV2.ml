(* generated from app.bsky.unspecced.getPostThreadOtherV2 *)

type value =
  | ThreadItemPost of App_bsky_unspecced_defs.thread_item_post
  | Unknown of Yojson.Safe.t

let value_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.unspecced.defs#threadItemPost" ->
        (match App_bsky_unspecced_defs.thread_item_post_of_yojson json with
         | Ok v -> Ok (ThreadItemPost v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let value_to_yojson = function
  | ThreadItemPost v ->
      (match App_bsky_unspecced_defs.thread_item_post_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.unspecced.defs#threadItemPost") :: fields)
       | other -> other)
  | Unknown j -> j

type thread_item =
  {
    uri: string;
    depth: int;
    value: value;
  }
[@@deriving yojson {strict= false}]

(** (NOTE: this endpoint is under development and WILL change without notice. Don't use it until it is moved out of `unspecced` or your application WILL break) Get additional posts under a thread e.g. replies hidden by threadgate. Based on an anchor post at any depth of the tree, returns top-level replies below that anchor. It does not include ancestors nor the anchor itself. This should be called after exhausting `app.bsky.unspecced.getPostThreadV2`. Does not require auth, but additional metadata and filtering will be applied for authed requests. *)
module Main = struct
  let nsid = "app.bsky.unspecced.getPostThreadOtherV2"

  type params =
  {
    anchor: string;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    thread: thread_item list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~anchor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {anchor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

