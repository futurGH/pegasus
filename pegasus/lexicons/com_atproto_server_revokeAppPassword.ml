(* generated from com.atproto.server.revokeAppPassword *)

(** Revoke an App Password by name. *)
module Main = struct
  let nsid = "com.atproto.server.revokeAppPassword"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {name: string} [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~name (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({name} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
