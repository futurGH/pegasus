(* generated from app.bsky.notification.updateSeen *)

(** Notify server that the requesting account has seen notifications. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.updateSeen"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {seen_at: string [@key "seenAt"]}
  [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~seen_at (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({seen_at} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
