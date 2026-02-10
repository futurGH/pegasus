(* generated from com.atproto.admin.disableAccountInvites *)

(** Disable an account from receiving new invite codes, but does not invalidate existing codes. *)
module Main = struct
  let nsid = "com.atproto.admin.disableAccountInvites"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {account: string; note: string option [@default None]}
  [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~account ?note (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({account; note} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
