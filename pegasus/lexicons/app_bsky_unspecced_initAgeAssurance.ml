(* generated from app.bsky.unspecced.initAgeAssurance *)

(** Initiate age assurance for an account. This is a one-time action that will start the process of verifying the user's age. *)
module Main = struct
  let nsid = "app.bsky.unspecced.initAgeAssurance"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input =
    {email: string; language: string; country_code: string [@key "countryCode"]}
  [@@deriving yojson {strict= false}]

  type output = App_bsky_unspecced_defs.age_assurance_state
  [@@deriving yojson {strict= false}]

  let call ~email ~language ~country_code (client : Hermes.client) :
      output Lwt.t =
    let params = () in
    let input = Some ({email; language; country_code} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
