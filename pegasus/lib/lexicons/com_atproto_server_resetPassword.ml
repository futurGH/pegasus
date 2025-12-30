(* generated from com.atproto.server.resetPassword *)

(** Reset a user account password using a token. *)
module Main = struct
  let nsid = "com.atproto.server.resetPassword"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      token: string;
      password: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~token
      ~password
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({token; password} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

