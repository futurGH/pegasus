(* generated from app.bsky.contact.dismissMatch *)

(** Removes a match that was found via contact import. It shouldn't appear again if the same contact is re-imported. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.contact.dismissMatch"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      subject: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
let output_of_yojson _ = Ok ()
let output_to_yojson () = `Assoc []

  let call
      ~subject
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({subject} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

