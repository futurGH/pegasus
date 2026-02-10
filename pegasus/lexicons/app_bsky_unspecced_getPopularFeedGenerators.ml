(* generated from app.bsky.unspecced.getPopularFeedGenerators *)

(** An unspecced view of globally popular feed generators. *)
module Main = struct
  let nsid = "app.bsky.unspecced.getPopularFeedGenerators"

  type params =
    { limit: int option [@default None]
    ; cursor: string option [@default None]
    ; query: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; feeds: App_bsky_feed_defs.generator_view list }
  [@@deriving yojson {strict= false}]

  let call ?limit ?cursor ?query (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor; query} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
