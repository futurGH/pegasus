(* generated from com.atproto.admin.updateAccountPassword *)

(** Update the password for a user account as an administrator. *)
module Main = struct
  let nsid = "com.atproto.admin.updateAccountPassword"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      did: string;
      password: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~did
      ~password
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({did; password} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

