(* generated from com.atproto.repo.listMissingBlobs *)

type record_blob =
  {
    cid: string;
    record_uri: string [@key "recordUri"];
  }
[@@deriving yojson {strict= false}]

(** Returns a list of missing blobs for the requesting account. Intended to be used in the account migration flow. *)
module Main = struct
  let nsid = "com.atproto.repo.listMissingBlobs"

  type params =
  {
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    blobs: record_blob list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

