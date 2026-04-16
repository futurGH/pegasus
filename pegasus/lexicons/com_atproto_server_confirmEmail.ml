(* generated from com.atproto.server.confirmEmail *)

(** Confirm an email using a token from com.atproto.server.requestEmailConfirmation. *)
module Main = struct
  let nsid = "com.atproto.server.confirmEmail"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      email: string;
      token: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~email
      ~token
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({email; token} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

