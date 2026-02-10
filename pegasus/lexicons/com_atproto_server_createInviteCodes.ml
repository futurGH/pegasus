(* generated from com.atproto.server.createInviteCodes *)

type account_codes = {account: string; codes: string list}
[@@deriving yojson {strict= false}]

(** Create invite codes. *)
module Main = struct
  let nsid = "com.atproto.server.createInviteCodes"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input =
    { code_count: int [@key "codeCount"]
    ; use_count: int [@key "useCount"]
    ; for_accounts: string list option [@key "forAccounts"] [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {codes: account_codes list} [@@deriving yojson {strict= false}]

  let call ~code_count ~use_count ?for_accounts (client : Hermes.client) :
      output Lwt.t =
    let params = () in
    let input =
      Some ({code_count; use_count; for_accounts} |> input_to_yojson)
    in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
