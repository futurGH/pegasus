(* generated from app.bsky.notification.putPreferences *)

(** Set notification-related preferences for an account. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.putPreferences"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      priority: bool;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~priority
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({priority} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

