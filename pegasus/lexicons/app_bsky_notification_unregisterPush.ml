(* generated from app.bsky.notification.unregisterPush *)

(** The inverse of registerPush - inform a specified service that push notifications should no longer be sent to the given token for the requesting account. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.unregisterPush"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      service_did: string [@key "serviceDid"];
      token: string;
      platform: string;
      app_id: string [@key "appId"];
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~service_did
      ~token
      ~platform
      ~app_id
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({service_did; token; platform; app_id} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

