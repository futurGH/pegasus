(* generated from app.bsky.notification.getPreferences *)

(** Get notification-related preferences for an account. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.getPreferences"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output = {preferences: App_bsky_notification_defs.preferences}
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
