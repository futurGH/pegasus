(* generated from app.bsky.feed.getQuotes *)

(** Get a list of quotes for a given post. *)
module Main = struct
  let nsid = "app.bsky.feed.getQuotes"

  type params =
    { uri: string
    ; cid: string option [@default None]
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { uri: string
    ; cid: string option [@default None]
    ; cursor: string option [@default None]
    ; posts: App_bsky_feed_defs.post_view list }
  [@@deriving yojson {strict= false}]

  let call ~uri ?cid ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {uri; cid; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
