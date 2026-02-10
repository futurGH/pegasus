(* generated from app.bsky.actor.putPreferences *)

(** Set the private preferences attached to the account. *)
module Main = struct
  let nsid = "app.bsky.actor.putPreferences"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {preferences: App_bsky_actor_defs.preferences}
  [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~preferences (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({preferences} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
