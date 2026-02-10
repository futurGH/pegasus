(* generated from com.atproto.identity.resolveDid *)

(** Resolves DID to DID document. Does not bi-directionally verify handle. *)
module Main = struct
  let nsid = "com.atproto.identity.resolveDid"

  type params = {did: string} [@@deriving yojson {strict= false}]

  type output = {did_doc: Yojson.Safe.t [@key "didDoc"]}
  [@@deriving yojson {strict= false}]

  let call ~did (client : Hermes.client) : output Lwt.t =
    let params : params = {did} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
