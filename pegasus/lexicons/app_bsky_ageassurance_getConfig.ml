(* generated from app.bsky.ageassurance.getConfig *)

(** Returns Age Assurance configuration for use on the client. *)
module Main = struct
  let nsid = "app.bsky.ageassurance.getConfig"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output = App_bsky_ageassurance_defs.config
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
