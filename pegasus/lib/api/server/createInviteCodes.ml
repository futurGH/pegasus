open Lexicons.Com.Atproto.Server.CreateInviteCodes
open Main

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {code_count; use_count; for_accounts} =
        Xrpc.parse_body req input_of_yojson
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
      Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson {codes} )
