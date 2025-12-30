(* generated from app.bsky.bookmark.getBookmarks *)

(** Gets views of records bookmarked by the authenticated user. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.bookmark.getBookmarks"

  type params =
  {
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    bookmarks: App_bsky_bookmark_defs.bookmark_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

