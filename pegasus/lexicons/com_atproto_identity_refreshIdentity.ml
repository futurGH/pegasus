(* generated from com.atproto.identity.refreshIdentity *)

(** Request that the server re-resolve an identity (DID and handle). The server may ignore this request, or require authentication, depending on the role, implementation, and policy of the server. *)
module Main = struct
  let nsid = "com.atproto.identity.refreshIdentity"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {identifier: string} [@@deriving yojson {strict= false}]

  type output = Com_atproto_identity_defs.identity_info
  [@@deriving yojson {strict= false}]

  let call ~identifier (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({identifier} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
