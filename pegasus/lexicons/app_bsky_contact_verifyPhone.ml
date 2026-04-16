(* generated from app.bsky.contact.verifyPhone *)

(** Verifies control over a phone number with a code received via SMS and starts a contact import session. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.contact.verifyPhone"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      phone: string;
      code: string;
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    token: string;
  }
[@@deriving yojson {strict= false}]

  let call
      ~phone
      ~code
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({phone; code} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

