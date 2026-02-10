(* generated from com.atproto.admin.deleteAccount *)

(** Delete a user account as an administrator. *)
module Main = struct
  let nsid = "com.atproto.admin.deleteAccount"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {did: string} [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~did (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({did} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
