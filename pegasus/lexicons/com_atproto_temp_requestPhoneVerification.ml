(* generated from com.atproto.temp.requestPhoneVerification *)

(** Request a verification code to be sent to the supplied phone number *)
module Main = struct
  let nsid = "com.atproto.temp.requestPhoneVerification"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      phone_number: string [@key "phoneNumber"];
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~phone_number
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({phone_number} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

