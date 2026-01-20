let pending_session_expiry_ms = 5 * 60 * 1000

let email_code_expiry_ms = 10 * 60 * 1000

module Types = struct
  type two_factor_method = TOTP | Email | BackupCode | SecurityKey

  type two_factor_status =
    { totp_enabled: bool
    ; email_2fa_enabled: bool
    ; backup_codes_remaining: int
    ; security_keys_count: int }
  [@@deriving yojson {strict= false}]

  type pending_2fa =
    { id: int
    ; session_token: string
    ; did: string
    ; password_verified_at: int
    ; expires_at: int
    ; email_code: string option
    ; email_code_expires_at: int option
    ; created_at: int }

  type available_methods = Frontend.LoginPage.two_fa_methods =
    {totp: bool; email: bool; backup_code: bool; security_key: bool}
  [@@deriving yojson {strict= false}]
end

open Types

module Queries = struct
  let insert_pending_2fa =
    [%rapper
      execute
        {sql| INSERT INTO pending_2fa (session_token, did, password_verified_at, expires_at, created_at)
              VALUES (%string{session_token}, %string{did}, %int{password_verified_at}, %int{expires_at}, %int{created_at})
        |sql}]

  let get_pending_2fa session_token now =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @string{session_token}, @string{did}, @int{password_verified_at},
                     @int{expires_at}, @string?{email_code}, @int?{email_code_expires_at}, @int{created_at}
              FROM pending_2fa WHERE session_token = %string{session_token} AND expires_at > %int{now}
        |sql}
        record_out]
      ~session_token ~now

  let get_pending_2fa_for_did did now =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @string{session_token}, @string{did}, @int{password_verified_at},
                     @int{expires_at}, @string?{email_code}, @int?{email_code_expires_at}, @int{created_at}
              FROM pending_2fa WHERE did = %string{did} AND expires_at > %int{now}
        |sql}
        record_out]
      ~did ~now

  let update_email_code =
    [%rapper
      execute
        {sql| UPDATE pending_2fa SET email_code = %string{email_code}, email_code_expires_at = %int{email_code_expires_at}
              WHERE session_token = %string{session_token}
        |sql}]

  let delete_pending_2fa =
    [%rapper
      execute
        {sql| DELETE FROM pending_2fa WHERE session_token = %string{session_token}
        |sql}]

  let get_email_2fa_enabled did =
    [%rapper
      get_opt
        {sql| SELECT @int{email_2fa_enabled} FROM actors WHERE did = %string{did}
        |sql}]
      did

  let set_email_2fa_enabled =
    [%rapper
      execute
        {sql| UPDATE actors SET email_2fa_enabled = %int{enabled}
              WHERE did = %string{did}
        |sql}]

  let is_2fa_enabled =
    [%rapper
      get_opt
        {sql| SELECT CASE
                  WHEN totp_verified_at IS NOT NULL
                    OR email_2fa_enabled = 1
                    OR EXISTS(SELECT 1 FROM security_keys WHERE security_keys.did = actors.did AND security_keys.verified_at IS NOT NULL)
                  THEN 1
                  ELSE 0
              END AS @int{result}
              FROM actors
              WHERE did = %string{did} |sql}]
end

let generate_session_token () =
  let () = Mirage_crypto_rng_unix.use_default () in
  let token = Mirage_crypto_rng_unix.getrandom 32 in
  Base64.(encode_string ~alphabet:uri_safe_alphabet ~pad:false token)

let is_2fa_enabled ~did db =
  match%lwt Util.Sqlite.use_pool db @@ Queries.is_2fa_enabled ~did with
  | Some 1 ->
      Lwt.return_true
  | _ ->
      Lwt.return_false

let get_status ~did db =
  let%lwt totp_enabled = Totp.is_enabled ~did db in
  let%lwt email_2fa =
    match%lwt Util.Sqlite.use_pool db @@ Queries.get_email_2fa_enabled ~did with
    | Some 1 ->
        Lwt.return_true
    | _ ->
        Lwt.return_false
  in
  let%lwt backup_count = Totp.Backup_codes.get_remaining_count ~did db in
  let%lwt security_keys_count =
    Security_key.count_verified_security_keys ~did db
  in
  Lwt.return
    { totp_enabled
    ; email_2fa_enabled= email_2fa
    ; backup_codes_remaining= backup_count
    ; security_keys_count }

let get_available_methods ~did db =
  let%lwt totp_enabled = Totp.is_enabled ~did db in
  let%lwt email_2fa =
    match%lwt Util.Sqlite.use_pool db @@ Queries.get_email_2fa_enabled ~did with
    | Some 1 ->
        Lwt.return_true
    | _ ->
        Lwt.return_false
  in
  let%lwt has_backup = Totp.Backup_codes.has_backup_codes ~did db in
  let%lwt has_security_key = Security_key.has_security_keys ~did db in
  Lwt.return
    { totp= totp_enabled
    ; email= email_2fa
    ; backup_code= has_backup
    ; security_key= has_security_key }

(* create a pending 2FA session after password verification *)
let create_pending_session ~did db =
  let session_token = generate_session_token () in
  let now = Util.Time.now_ms () in
  let expires_at = now + pending_session_expiry_ms in
  let%lwt () =
    Util.Sqlite.use_pool db
    @@ Queries.insert_pending_2fa ~session_token ~did ~password_verified_at:now
         ~expires_at ~created_at:now
  in
  Lwt.return session_token

let get_pending_session ~session_token db =
  let now = Util.Time.now_ms () in
  Util.Sqlite.use_pool db @@ Queries.get_pending_2fa session_token now

let get_pending_session_for_did ~did db =
  let now = Util.Time.now_ms () in
  Util.Sqlite.use_pool db @@ Queries.get_pending_2fa_for_did did now

let delete_pending_session ~session_token db =
  Util.Sqlite.use_pool db @@ Queries.delete_pending_2fa ~session_token

let send_email_code ~session_token ~actor db =
  let code = Util.make_code () in
  let now = Util.Time.now_ms () in
  let expires_at = now + email_code_expiry_ms in
  let%lwt () =
    Util.Sqlite.use_pool db
    @@ Queries.update_email_code ~session_token ~email_code:code
         ~email_code_expires_at:expires_at
  in
  let subject = "Your login verification code" in
  let body =
    Emails.TwoFactorAuth.make ~handle:actor.Data_store.Types.handle ~code
  in
  let recipients = [Letters.To actor.email] in
  let%lwt () = Util.send_email_or_log ~recipients ~subject ~body in
  Lwt.return_unit

let _verify_email_code ~code ~session =
  match (session.email_code, session.email_code_expires_at) with
  | Some stored_code, Some expires_at ->
      let now = Util.Time.now_ms () in
      if now > expires_at then Lwt.return_error "Email code expired"
      else if stored_code = code then Lwt.return_ok session.did
      else Lwt.return_error "Invalid code"
  | _ ->
      Lwt.return_error "No email code sent for this session"

let verify_email_code_by_token ~session_token ~code db =
  match%lwt get_pending_session ~session_token db with
  | None ->
      Lwt.return_error "Invalid or expired session"
  | Some pending ->
      _verify_email_code ~code ~session:pending

let verify_email_code_by_did ~did ~code db =
  match%lwt get_pending_session_for_did ~did db with
  | None ->
      Lwt.return_error "Invalid or expired session"
  | Some pending ->
      _verify_email_code ~code ~session:pending

let verify_totp_code ~session_token ~code db =
  match%lwt get_pending_session ~session_token db with
  | None ->
      Lwt.return_error "Invalid or expired session"
  | Some pending ->
      let%lwt valid = Totp.verify_login_code ~did:pending.did ~code db in
      if valid then Lwt.return_ok pending.did
      else Lwt.return_error "Invalid TOTP code"

let verify_backup_code ~session_token ~code db =
  match%lwt get_pending_session ~session_token db with
  | None ->
      Lwt.return_error "Invalid or expired session"
  | Some pending ->
      let%lwt valid =
        Totp.Backup_codes.verify_and_consume ~did:pending.did ~code db
      in
      if valid then Lwt.return_ok pending.did
      else Lwt.return_error "Invalid backup code"

let enable_email_2fa ~did db =
  Util.Sqlite.use_pool db @@ Queries.set_email_2fa_enabled ~did ~enabled:1

let disable_email_2fa ~did db =
  Util.Sqlite.use_pool db @@ Queries.set_email_2fa_enabled ~did ~enabled:0

let is_email_2fa_enabled ~did db =
  match%lwt Util.Sqlite.use_pool db @@ Queries.get_email_2fa_enabled ~did with
  | Some 1 ->
      Lwt.return_true
  | _ ->
      Lwt.return_false

let verify_code ~did ~code db =
  let%lwt sk_valid = Security_key.verify_login ~did ~code db in
  if sk_valid then Lwt.return_ok ()
  else
    let%lwt totp_valid = Totp.verify_login_code ~did ~code db in
    if totp_valid then Lwt.return_ok ()
    else
      let%lwt backup_valid =
        Totp.Backup_codes.verify_and_consume ~did ~code db
      in
      if backup_valid then Lwt.return_ok ()
      else
        match%lwt verify_email_code_by_did ~did ~code db with
        | Ok _ ->
            Lwt.return_ok ()
        | Error e ->
            Lwt.return_error e

let verify_code_with_pending_session ~(pending : Types.pending_2fa) ~code db =
  let did = pending.did in
  let%lwt sk_valid = Security_key.verify_login ~did ~code db in
  if sk_valid then Lwt.return_ok did
  else
    let%lwt totp_valid = Totp.verify_login_code ~did ~code db in
    if totp_valid then Lwt.return_ok did
    else
      let%lwt backup_valid =
        Totp.Backup_codes.verify_and_consume ~did ~code db
      in
      if backup_valid then Lwt.return_ok did
      else
        match%lwt _verify_email_code ~code ~session:pending with
        | Ok did ->
            Lwt.return_ok did
        | Error e ->
            Lwt.return_error e

let verify_code_by_session_token ~session_token ~code db =
  match%lwt get_pending_session ~session_token db with
  | None ->
      Lwt.return_error "Invalid or expired session"
  | Some pending ->
      verify_code_with_pending_session ~pending ~code db
