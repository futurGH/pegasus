(* generated from com.atproto.label.queryLabels *)

(** Find labels relevant to the provided AT-URI patterns. Public endpoint for moderation services, though may return different or additional results with auth. *)
module Main = struct
  let nsid = "com.atproto.label.queryLabels"

  type params =
  {
    uri_patterns: string list [@key "uriPatterns"] [@of_yojson Hermes_util.query_string_list_of_yojson] [@to_yojson Hermes_util.query_string_list_to_yojson];
    sources: string list option [@default None] [@of_yojson Hermes_util.query_string_list_option_of_yojson] [@to_yojson Hermes_util.query_string_list_option_to_yojson];
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    labels: Com_atproto_label_defs.label list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~uri_patterns
      ?sources
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {uri_patterns; sources; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

