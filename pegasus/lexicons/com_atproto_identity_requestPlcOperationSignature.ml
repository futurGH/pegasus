(* generated from com.atproto.identity.requestPlcOperationSignature *)

(** Request an email with a code to in order to request a signed PLC operation. Requires Auth. *)
module Main = struct
  let nsid = "com.atproto.identity.requestPlcOperationSignature"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output = unit

  let output_of_yojson _ = Ok ()

  let call (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = None in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
