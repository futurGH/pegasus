(* generated from app.bsky.notification.putActivitySubscription *)

(** Puts an activity subscription entry. The key should be omitted for creation and provided for updates. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.putActivitySubscription"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input =
    { subject: string
    ; activity_subscription: App_bsky_notification_defs.activity_subscription
          [@key "activitySubscription"] }
  [@@deriving yojson {strict= false}]

  type output =
    { subject: string
    ; activity_subscription:
        App_bsky_notification_defs.activity_subscription option
          [@key "activitySubscription"] [@default None] }
  [@@deriving yojson {strict= false}]

  let call ~subject ~activity_subscription (client : Hermes.client) :
      output Lwt.t =
    let params = () in
    let input = Some ({subject; activity_subscription} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
