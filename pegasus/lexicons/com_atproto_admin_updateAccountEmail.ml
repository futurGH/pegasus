(* generated from com.atproto.admin.updateAccountEmail *)

(** Administrative action to update an account's email. *)
module Main = struct
  let nsid = "com.atproto.admin.updateAccountEmail"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      account: string;
      email: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~account
      ~email
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({account; email} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

