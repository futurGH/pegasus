(* generated from com.atproto.temp.revokeAccountCredentials *)

(** Revoke sessions, password, and app passwords associated with account. May be resolved by a password reset. *)
module Main = struct
  let nsid = "com.atproto.temp.revokeAccountCredentials"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      account: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~account
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({account} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

