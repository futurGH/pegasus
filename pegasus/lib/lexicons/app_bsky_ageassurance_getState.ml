(* generated from app.bsky.ageassurance.getState *)

(** Returns server-computed Age Assurance state, if available, and any additional metadata needed to compute Age Assurance state client-side. *)
module Main = struct
  let nsid = "app.bsky.ageassurance.getState"

  type params =
  {
    country_code: string [@key "countryCode"];
    region_code: string option [@key "regionCode"] [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    state: App_bsky_ageassurance_defs.state;
    metadata: App_bsky_ageassurance_defs.state_metadata;
  }
[@@deriving yojson {strict= false}]

  let call
      ~country_code
      ?region_code
      (client : Hermes.client) : output Lwt.t =
    let params : params = {country_code; region_code} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

