(* generated from com.atproto.identity.submitPlcOperation *)

(** Validates a PLC operation to ensure that it doesn't violate a service's constraints or get the identity into a bad state, then submits it to the PLC registry *)
module Main = struct
  let nsid = "com.atproto.identity.submitPlcOperation"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {operation: Yojson.Safe.t} [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~operation (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({operation} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
