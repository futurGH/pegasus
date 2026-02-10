(* generated from com.atproto.identity.updateHandle *)

(** Updates the current account's handle. Verifies handle validity, and updates did:plc document if necessary. Implemented by PDS, and requires auth. *)
module Main = struct
  let nsid = "com.atproto.identity.updateHandle"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {handle: string} [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~handle (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({handle} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
