(* generated from app.bsky.unspecced.getTrends *)

(** Get the current trends on the network *)
module Main = struct
  let nsid = "app.bsky.unspecced.getTrends"

  type params =
  {
    limit: int option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    trends: App_bsky_unspecced_defs.trend_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?limit
      (client : Hermes.client) : output Lwt.t =
    let params : params = {limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

