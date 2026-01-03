(* generated from app.bsky.contact.sendNotification *)

(** System endpoint to send notifications related to contact imports. Requires role authentication. *)
module Main = struct
  let nsid = "app.bsky.contact.sendNotification"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      from: string;
      to_: string [@key "to"];
    }
  [@@deriving yojson {strict= false}]

  type output = unit
let output_of_yojson _ = Ok ()
let output_to_yojson () = `Assoc []

  let call
      ~from
      ~to_
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({from; to_} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

