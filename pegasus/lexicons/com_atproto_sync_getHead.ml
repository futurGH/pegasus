(* generated from com.atproto.sync.getHead *)

(** DEPRECATED - please use com.atproto.sync.getLatestCommit instead *)
module Main = struct
  let nsid = "com.atproto.sync.getHead"

  type params = {did: string} [@@deriving yojson {strict= false}]

  type output = {root: string} [@@deriving yojson {strict= false}]

  let call ~did (client : Hermes.client) : output Lwt.t =
    let params : params = {did} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
