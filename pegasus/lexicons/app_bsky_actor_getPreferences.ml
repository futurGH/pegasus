(* generated from app.bsky.actor.getPreferences *)

(** Get private preferences attached to the current account. Expected use is synchronization between multiple devices, and import/export during account migration. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.actor.getPreferences"

  type params = unit
  let params_to_yojson () = `Assoc []

  type output =
  {
    preferences: App_bsky_actor_defs.preferences;
  }
[@@deriving yojson {strict= false}]

  let call
      (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end

