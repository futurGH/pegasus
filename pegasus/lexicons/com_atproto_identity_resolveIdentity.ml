(* generated from com.atproto.identity.resolveIdentity *)

(** Resolves an identity (DID or Handle) to a full identity (DID document and verified handle). *)
module Main = struct
  let nsid = "com.atproto.identity.resolveIdentity"

  type params = {identifier: string} [@@deriving yojson {strict= false}]

  type output = Com_atproto_identity_defs.identity_info
  [@@deriving yojson {strict= false}]

  let call ~identifier (client : Hermes.client) : output Lwt.t =
    let params : params = {identifier} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
