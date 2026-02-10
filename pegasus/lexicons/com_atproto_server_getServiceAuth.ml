(* generated from com.atproto.server.getServiceAuth *)

(** Get a signed token on behalf of the requesting DID for the requested service. *)
module Main = struct
  let nsid = "com.atproto.server.getServiceAuth"

  type params =
    { aud: string
    ; exp: int option [@default None]
    ; lxm: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {token: string} [@@deriving yojson {strict= false}]

  let call ~aud ?exp ?lxm (client : Hermes.client) : output Lwt.t =
    let params : params = {aud; exp; lxm} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
