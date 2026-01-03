(* generated from app.bsky.feed.searchPosts *)

(** Find posts matching search criteria, returning views of those posts. Note that this API endpoint may require authentication (eg, not public) for some service providers and implementations. *)
module Main = struct
  let nsid = "app.bsky.feed.searchPosts"

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
    tag: string list option [@default None] [@of_yojson Hermes_util.query_string_list_option_of_yojson] [@to_yojson Hermes_util.query_string_list_option_to_yojson];
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    hits_total: int option [@key "hitsTotal"] [@default None];
    posts: App_bsky_feed_defs.post_view list;
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
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {q; sort; since; until; mentions; author; lang; domain; url; tag; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

