(* generated from com.atproto.admin.updateAccountSigningKey *)

(** Administrative action to update an account's signing key in their Did document. *)
module Main = struct
  let nsid = "com.atproto.admin.updateAccountSigningKey"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      did: string;
      signing_key: string [@key "signingKey"];
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~did
      ~signing_key
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({did; signing_key} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

