(* generated from app.bsky.unspecced.getAgeAssuranceState *)

(** Returns the current state of the age assurance process for an account. This is used to check if the user has completed age assurance or if further action is required. *)
module Main = struct
  let nsid = "app.bsky.unspecced.getAgeAssuranceState"

  type params = unit
  let params_to_yojson () = `Assoc []

  type output = App_bsky_unspecced_defs.age_assurance_state
[@@deriving yojson {strict= false}]

  let call
      (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end

