(* generated from com.atproto.identity.signPlcOperation *)

(** Signs a PLC operation to update some value(s) in the requesting DID's document. *)
module Main = struct
  let nsid = "com.atproto.identity.signPlcOperation"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input =
    { token: string option [@default None]
    ; rotation_keys: string list option [@key "rotationKeys"] [@default None]
    ; also_known_as: string list option [@key "alsoKnownAs"] [@default None]
    ; verification_methods: Yojson.Safe.t option
          [@key "verificationMethods"] [@default None]
    ; services: Yojson.Safe.t option [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {operation: Yojson.Safe.t} [@@deriving yojson {strict= false}]

  let call ?token ?rotation_keys ?also_known_as ?verification_methods ?services
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input =
      Some
        ( {token; rotation_keys; also_known_as; verification_methods; services}
        |> input_to_yojson )
    in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
