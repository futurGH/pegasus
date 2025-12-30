(* generated from com.atproto.server.requestEmailConfirmation *)

(** Request an email with a code to confirm ownership of email. *)
module Main = struct
  let nsid = "com.atproto.server.requestEmailConfirmation"

  type params = unit
  let params_to_yojson () = `Assoc []

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = None in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

