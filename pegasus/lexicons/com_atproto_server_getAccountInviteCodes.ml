(* generated from com.atproto.server.getAccountInviteCodes *)

(** Get all invite codes for the current account. Requires auth. *)
module Main = struct
  let nsid = "com.atproto.server.getAccountInviteCodes"

  type params =
    { include_used: bool option [@key "includeUsed"] [@default None]
    ; create_available: bool option [@key "createAvailable"] [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {codes: Com_atproto_server_defs.invite_code list}
  [@@deriving yojson {strict= false}]

  let call ?include_used ?create_available (client : Hermes.client) :
      output Lwt.t =
    let params : params = {include_used; create_available} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
