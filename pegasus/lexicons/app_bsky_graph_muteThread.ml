(* generated from app.bsky.graph.muteThread *)

(** Mutes a thread preventing notifications from the thread and any of its children. Mutes are private in Bluesky. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.graph.muteThread"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      root: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~root
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({root} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

