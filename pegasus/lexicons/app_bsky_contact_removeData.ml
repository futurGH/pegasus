(* generated from app.bsky.contact.removeData *)

(** Removes all stored hashes used for contact matching, existing matches, and sync status. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.contact.removeData"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input = unit
  let input_of_yojson _ = Ok ()
  let input_to_yojson () = `Assoc []

  type output = unit
let output_of_yojson _ = Ok ()
let output_to_yojson () = `Assoc []

  let call
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some (input_to_yojson ()) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

