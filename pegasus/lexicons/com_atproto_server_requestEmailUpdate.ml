(* generated from com.atproto.server.requestEmailUpdate *)

(** Request a token in order to update email. *)
module Main = struct
  let nsid = "com.atproto.server.requestEmailUpdate"

  type params = unit
  let params_to_yojson () = `Assoc []

  type output =
  {
    token_required: bool [@key "tokenRequired"];
  }
[@@deriving yojson {strict= false}]

  let call
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = None in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

