(* generated from app.bsky.draft.getDrafts *)

(** Gets views of user drafts. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.draft.getDrafts"

  type params =
  {
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@xrpc_query]

  type output =
  {
    cursor: string option [@default None];
    drafts: App_bsky_draft_defs.draft_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

