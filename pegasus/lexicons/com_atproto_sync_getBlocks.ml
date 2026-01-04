(* generated from com.atproto.sync.getBlocks *)

(** Get data blocks from a given repo, by CID. For example, intermediate MST nodes, or records. Does not require auth; implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.sync.getBlocks"

  type params =
  {
    did: string;
    cids: string list [@of_yojson Hermes_util.query_string_list_of_yojson] [@to_yojson Hermes_util.query_string_list_to_yojson];
  }
[@@deriving yojson {strict= false}]

  (** raw bytes output with content type *)
  type output = bytes * string

  let call
      ~did
      ~cids
      (client : Hermes.client) : output Lwt.t =
    let params : params = {did; cids} in
    Hermes.query_bytes client nsid (params_to_yojson params)
end

