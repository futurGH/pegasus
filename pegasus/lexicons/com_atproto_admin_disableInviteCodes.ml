(* generated from com.atproto.admin.disableInviteCodes *)

(** Disable some set of codes and/or all codes associated with a set of users. *)
module Main = struct
  let nsid = "com.atproto.admin.disableInviteCodes"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input =
    { codes: string list option [@default None]
    ; accounts: string list option [@default None] }
  [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ?codes ?accounts (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({codes; accounts} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
