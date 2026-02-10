(* generated from com.atproto.temp.fetchLabels *)

(** DEPRECATED: use queryLabels or subscribeLabels instead -- Fetch all labels from a labeler created after a certain date. *)
module Main = struct
  let nsid = "com.atproto.temp.fetchLabels"

  type params =
    {since: int option [@default None]; limit: int option [@default None]}
  [@@deriving yojson {strict= false}]

  type output = {labels: Com_atproto_label_defs.label list}
  [@@deriving yojson {strict= false}]

  let call ?since ?limit (client : Hermes.client) : output Lwt.t =
    let params : params = {since; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
