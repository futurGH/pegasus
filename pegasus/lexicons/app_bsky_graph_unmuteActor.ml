(* generated from app.bsky.graph.unmuteActor *)

(** Unmutes the specified account. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.graph.unmuteActor"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {actor: string} [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~actor (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({actor} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
