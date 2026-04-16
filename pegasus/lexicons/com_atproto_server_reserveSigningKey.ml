(* generated from com.atproto.server.reserveSigningKey *)

(** Reserve a repo signing key, for use with account creation. Necessary so that a DID PLC update operation can be constructed during an account migraiton. Public and does not require auth; implemented by PDS. NOTE: this endpoint may change when full account migration is implemented. *)
module Main = struct
  let nsid = "com.atproto.server.reserveSigningKey"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      did: string option [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    signing_key: string [@key "signingKey"];
  }
[@@deriving yojson {strict= false}]

  let call
      ?did
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({did} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

