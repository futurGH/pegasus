(* generated from com.atproto.identity.resolveHandle *)

(** Resolves an atproto handle (hostname) to a DID. Does not necessarily bi-directionally verify against the the DID document. *)
module Main = struct
  let nsid = "com.atproto.identity.resolveHandle"

  type params = {handle: string} [@@deriving yojson {strict= false}]

  type output = {did: string} [@@deriving yojson {strict= false}]

  let call ~handle (client : Hermes.client) : output Lwt.t =
    let params : params = {handle} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
