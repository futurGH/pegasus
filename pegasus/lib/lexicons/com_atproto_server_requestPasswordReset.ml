(* generated from com.atproto.server.requestPasswordReset *)

(** Initiate a user account password reset via email. *)
module Main = struct
  let nsid = "com.atproto.server.requestPasswordReset"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      email: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~email
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({email} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

