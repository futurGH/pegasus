(* generated from com.atproto.admin.updateAccountHandle *)

(** Administrative action to update an account's handle. *)
module Main = struct
  let nsid = "com.atproto.admin.updateAccountHandle"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      did: string;
      handle: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~did
      ~handle
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({did; handle} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

