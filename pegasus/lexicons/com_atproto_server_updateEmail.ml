(* generated from com.atproto.server.updateEmail *)

(** Update an account's email. *)
module Main = struct
  let nsid = "com.atproto.server.updateEmail"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      email: string;
      email_auth_factor: bool option [@key "emailAuthFactor"] [@default None];
      token: string option [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~email
      ?email_auth_factor
      ?token
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({email; email_auth_factor; token} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

