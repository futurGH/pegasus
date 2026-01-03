(* generated from com.atproto.sync.getLatestCommit *)

(** Get the current commit CID & revision of the specified repo. Does not require auth. *)
module Main = struct
  let nsid = "com.atproto.sync.getLatestCommit"

  type params =
  {
    did: string;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cid: string;
    rev: string;
  }
[@@deriving yojson {strict= false}]

  let call
      ~did
      (client : Hermes.client) : output Lwt.t =
    let params : params = {did} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

