type request =
  { code_count: int [@key "codeCount"] [@default 1]
  ; use_count: int [@key "useCount"]
  ; for_accounts: string list option [@key "forAccounts"] [@default None] }
[@@deriving yojson {strict= false}]

type account_codes = {account: string; codes: string list} [@@deriving yojson]

type response = {codes: account_codes list} [@@deriving yojson]

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {code_count; use_count; for_accounts} =
        Xrpc.parse_body req request_of_yojson
      in
      let code_count = Int.max 1 (Int.min code_count 100) in
      let use_count = Int.max 1 (Int.min use_count 100) in
      let accounts = Option.value for_accounts ~default:["admin"] in
      let%lwt codes =
        Lwt_list.map_s
          (fun account ->
            let%lwt account_codes =
              Lwt_list.map_s
                (fun _ ->
                  let code = CreateInviteCode.generate_code account in
                  let%lwt () =
                    Data_store.create_invite ~code ~did:account
                      ~remaining:use_count db
                  in
                  Lwt.return code )
                (List.init code_count (fun i -> i))
            in
            Lwt.return {account; codes= account_codes} )
          accounts
      in
      Dream.json @@ Yojson.Safe.to_string @@ response_to_yojson {codes} )
