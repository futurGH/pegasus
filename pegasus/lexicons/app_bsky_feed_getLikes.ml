(* generated from app.bsky.feed.getLikes *)

type like =
  {
    indexed_at: string [@key "indexedAt"];
    created_at: string [@key "createdAt"];
    actor: App_bsky_actor_defs.profile_view;
  }
[@@deriving yojson {strict= false}]

(** Get like records which reference a subject (by AT-URI and CID). *)
module Main = struct
  let nsid = "app.bsky.feed.getLikes"

  type params =
  {
    uri: string;
    cid: string option [@default None];
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    uri: string;
    cid: string option [@default None];
    cursor: string option [@default None];
    likes: like list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~uri
      ?cid
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {uri; cid; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

