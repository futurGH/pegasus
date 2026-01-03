(* generated from app.bsky.graph.muteActorList *)

(** Creates a mute relationship for the specified list of accounts. Mutes are private in Bluesky. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.graph.muteActorList"

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

