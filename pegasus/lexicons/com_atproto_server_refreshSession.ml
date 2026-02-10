(* generated from com.atproto.server.refreshSession *)

(** Refresh an authentication session. Requires auth using the 'refreshJwt' (not the 'accessJwt'). *)
module Main = struct
  let nsid = "com.atproto.server.refreshSession"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output =
    { access_jwt: string [@key "accessJwt"]
    ; refresh_jwt: string [@key "refreshJwt"]
    ; handle: string
    ; did: string
    ; did_doc: Yojson.Safe.t option [@key "didDoc"] [@default None]
    ; active: bool option [@default None]
    ; status: string option [@default None] }
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = None in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
