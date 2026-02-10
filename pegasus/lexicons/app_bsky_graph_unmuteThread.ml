(* generated from app.bsky.graph.unmuteThread *)

(** Unmutes the specified thread. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.graph.unmuteThread"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {root: string} [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~root (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({root} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
