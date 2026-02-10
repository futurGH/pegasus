(* generated from com.atproto.server.deactivateAccount *)

(** Deactivates a currently active account. Stops serving of repo, and future writes to repo until reactivated. Used to finalize account migration with the old host after the account has been activated on the new host. *)
module Main = struct
  let nsid = "com.atproto.server.deactivateAccount"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {delete_after: string option [@key "deleteAfter"] [@default None]}
  [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ?delete_after (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({delete_after} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
