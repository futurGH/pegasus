(* generated from com.atproto.admin.getAccountInfo *)

(** Get details about an account. *)
module Main = struct
  let nsid = "com.atproto.admin.getAccountInfo"

  type params = {did: string} [@@deriving yojson {strict= false}]

  type output = Com_atproto_admin_defs.account_view
  [@@deriving yojson {strict= false}]

  let call ~did (client : Hermes.client) : output Lwt.t =
    let params : params = {did} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
