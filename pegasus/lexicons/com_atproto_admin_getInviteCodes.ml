(* generated from com.atproto.admin.getInviteCodes *)

(** Get an admin view of invite codes. *)
module Main = struct
  let nsid = "com.atproto.admin.getInviteCodes"

  type params =
    { sort: string option [@default None]
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; codes: Com_atproto_server_defs.invite_code list }
  [@@deriving yojson {strict= false}]

  let call ?sort ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {sort; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
