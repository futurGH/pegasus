type count_response = {remaining: int} [@@deriving yojson {strict= false}]

type regenerate_response = {success: bool; codes: string list}
[@@deriving yojson {strict= false}]

let count_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      let%lwt count = Totp.Backup_codes.get_remaining_count ~did ctx.db in
      Dream.json @@ Yojson.Safe.to_string
      @@ count_response_to_yojson {remaining= count} )

let regenerate_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      (* only allow regeneration if 2FA is enabled *)
      let%lwt is_2fa_enabled = Two_factor.is_2fa_enabled ~did ctx.db in
      if not is_2fa_enabled then
        Errors.invalid_request "2FA must be enabled to generate backup codes"
      else
        let%lwt codes = Totp.Backup_codes.regenerate ~did ctx.db in
        Dream.json @@ Yojson.Safe.to_string
        @@ regenerate_response_to_yojson {success= true; codes} )
