(* generated from com.atproto.admin.getAccountInfos *)

(** Get details about some accounts. *)
module Main = struct
  let nsid = "com.atproto.admin.getAccountInfos"

  type params =
  {
    dids: string list [@of_yojson Hermes_util.query_string_list_of_yojson] [@to_yojson Hermes_util.query_string_list_to_yojson];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    infos: Com_atproto_admin_defs.account_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ~dids
      (client : Hermes.client) : output Lwt.t =
    let params : params = {dids} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

