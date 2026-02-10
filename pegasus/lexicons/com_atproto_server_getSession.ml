(* generated from com.atproto.server.getSession *)

(** Get information about the current auth session. Requires auth. *)
module Main = struct
  let nsid = "com.atproto.server.getSession"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output =
    { handle: string
    ; did: string
    ; email: string option [@default None]
    ; email_confirmed: bool option [@key "emailConfirmed"] [@default None]
    ; email_auth_factor: bool option [@key "emailAuthFactor"] [@default None]
    ; did_doc: Yojson.Safe.t option [@key "didDoc"] [@default None]
    ; active: bool option [@default None]
    ; status: string option [@default None] }
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
