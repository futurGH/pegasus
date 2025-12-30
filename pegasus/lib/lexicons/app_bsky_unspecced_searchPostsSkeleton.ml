(* generated from app.bsky.unspecced.searchPostsSkeleton *)

(** Backend Posts search, returns only skeleton *)
module Main = struct
  let nsid = "app.bsky.unspecced.searchPostsSkeleton"

  type params =
  {
    q: string;
    sort: string option [@default None];
    since: string option [@default None];
    until: string option [@default None];
    mentions: string option [@default None];
    author: string option [@default None];
    lang: string option [@default None];
    domain: string option [@default None];
    url: string option [@default None];
    tag: string list option [@default None];
    viewer: string option [@default None];
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    hits_total: int option [@key "hitsTotal"] [@default None];
    posts: App_bsky_unspecced_defs.skeleton_search_post list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~q
      ?sort
      ?since
      ?until
      ?mentions
      ?author
      ?lang
      ?domain
      ?url
      ?tag
      ?viewer
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {q; sort; since; until; mentions; author; lang; domain; url; tag; viewer; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

