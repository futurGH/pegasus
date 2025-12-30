(* generated from com.atproto.admin.searchAccounts *)

(** Get list of accounts that matches your search query. *)
module Main = struct
  let nsid = "com.atproto.admin.searchAccounts"

  type params =
  {
    email: string option [@default None];
    cursor: string option [@default None];
    limit: int option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    accounts: Com_atproto_admin_defs.account_view list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?email
      ?cursor
      ?limit
      (client : Hermes.client) : output Lwt.t =
    let params : params = {email; cursor; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

