(* generated from app.bsky.actor.status *)

type embed =
  | External of App_bsky_embed_external.main
  | Unknown of Yojson.Safe.t

let embed_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.embed.external" -> (
      match App_bsky_embed_external.main_of_yojson json with
      | Ok v ->
          Ok (External v)
      | Error e ->
          Error e )
    | _ ->
        Ok (Unknown json)
  with _ -> Error "failed to parse union"

let embed_to_yojson = function
  | External v -> (
    match App_bsky_embed_external.main_to_yojson v with
    | `Assoc fields ->
        `Assoc (("$type", `String "app.bsky.embed.external") :: fields)
    | other ->
        other )
  | Unknown j ->
      j

type main =
  { status: string
  ; embed: embed option [@default None]
  ; duration_minutes: int option [@key "durationMinutes"] [@default None]
  ; created_at: string [@key "createdAt"] }
[@@deriving yojson {strict= false}]

(** Advertises an account as currently offering live content. *)
let live = "app.bsky.actor.status#live"
