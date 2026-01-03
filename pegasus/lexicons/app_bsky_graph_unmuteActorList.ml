(* generated from app.bsky.graph.unmuteActorList *)

(** Unmutes the specified list of accounts. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.graph.unmuteActorList"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      list_: string [@key "list"];
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~list_
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({list_} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

