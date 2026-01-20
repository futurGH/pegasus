(* generated from app.bsky.ageassurance.begin *)

(** Initiate Age Assurance for an account. *)
module Main = struct
  let nsid = "app.bsky.ageassurance.begin"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input =
    { email: string
    ; language: string
    ; country_code: string [@key "countryCode"]
    ; region_code: string option [@key "regionCode"] [@default None] }
  [@@deriving yojson {strict= false}]

  type output = App_bsky_ageassurance_defs.state
  [@@deriving yojson {strict= false}]

  let call ~email ~language ~country_code ?region_code (client : Hermes.client)
      : output Lwt.t =
    let params = () in
    let input =
      Some ({email; language; country_code; region_code} |> input_to_yojson)
    in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
