(* generated from app.bsky.notification.registerPush *)

(** Register to receive push notifications, via a specified service, for the requesting account. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.registerPush"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input =
    { service_did: string [@key "serviceDid"]
    ; token: string
    ; platform: string
    ; app_id: string [@key "appId"]
    ; age_restricted: bool option [@key "ageRestricted"] [@default None] }
  [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~service_did ~token ~platform ~app_id ?age_restricted
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input =
      Some
        ( {service_did; token; platform; app_id; age_restricted}
        |> input_to_yojson )
    in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
