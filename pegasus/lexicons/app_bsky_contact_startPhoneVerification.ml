(* generated from app.bsky.contact.startPhoneVerification *)

(** Starts a phone verification flow. The phone passed will receive a code via SMS that should be passed to `app.bsky.contact.verifyPhone`. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.contact.startPhoneVerification"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      phone: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
let output_of_yojson _ = Ok ()
let output_to_yojson () = `Assoc []

  let call
      ~phone
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({phone} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

