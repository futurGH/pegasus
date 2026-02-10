(* generated from com.atproto.server.deleteAccount *)

(** Delete an actor's account with a token and password. Can only be called after requesting a deletion token. Requires auth. *)
module Main = struct
  let nsid = "com.atproto.server.deleteAccount"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {did: string; password: string; token: string}
  [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~did ~password ~token (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({did; password; token} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
