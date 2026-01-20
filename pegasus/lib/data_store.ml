open Util.Rapper

module Types = struct
  type actor =
    { id: int
    ; did: string
    ; handle: string
    ; email: string
    ; email_confirmed_at: int option
    ; password_hash: string
    ; signing_key: string
    ; preferences: Yojson.Safe.t
    ; created_at: int
    ; deactivated_at: int option
    ; auth_code: string option
    ; auth_code_expires_at: int option
    ; pending_email: string option
    ; email_2fa_enabled: int
    ; totp_secret: bytes option
    ; totp_verified_at: int option }

  type invite_code = {code: string; did: string; remaining: int}

  type firehose_event = {seq: int; time: int; t: string; data: bytes}

  type reserved_key =
    {key_did: string; did: string option; private_key: string; created_at: int}
end

open Types

module Queries = struct
  let create_actor =
    [%rapper
      execute
        {sql| INSERT INTO actors (
                did,
                handle,
                email,
                password_hash,
                signing_key,
                preferences,
                created_at
              ) VALUES (
                %string{did},
                %string{handle},
                %string{email},
                %string{password_hash},
                %string{signing_key},
                %Json{preferences},
                %int{created_at}
              )
        |sql}]

  let get_actor_by_identifier id =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @int?{email_confirmed_at}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}, @string?{auth_code}, @int?{auth_code_expires_at}, @string?{pending_email}, COALESCE(@int{email_2fa_enabled}, 0), @Blob?{totp_secret}, @int?{totp_verified_at}
              FROM actors WHERE did = %string{id} OR handle = %string{id} OR email = %string{id}
              LIMIT 1
        |sql}
        record_out]
      id

  let activate_actor =
    [%rapper
      execute
        {sql| UPDATE actors SET deactivated_at = NULL WHERE did = %string{did}
        |sql}]

  let deactivate_actor =
    [%rapper
      execute
        {sql| UPDATE actors SET deactivated_at = %int{deactivated_at} WHERE did = %string{did}
        |sql}]

  let delete_actor =
    [%rapper
      execute {sql| DELETE FROM actors WHERE did = %string{did}
        |sql}]

  let update_actor_handle =
    [%rapper
      execute
        {sql| UPDATE actors SET handle = %string{handle} WHERE did = %string{did}
        |sql}]

  let list_actors =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @int?{email_confirmed_at}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}, @string?{auth_code}, @int?{auth_code_expires_at}, @string?{pending_email}, COALESCE(@int{email_2fa_enabled}, 0), @Blob?{totp_secret}, @int?{totp_verified_at}
              FROM actors
              WHERE did > %string{cursor}
              AND deactivated_at IS NULL
              ORDER BY created_at DESC LIMIT %int{limit}
        |sql}
        record_out]

  let put_preferences =
    [%rapper
      execute
        {sql| UPDATE actors SET preferences = %Json{preferences} WHERE did = %string{did}
        |sql}]

  (* invites *)
  let create_invite =
    [%rapper
      execute
        {sql| INSERT INTO invite_codes (code, did, remaining)
              VALUES (%string{code}, %string{did}, %int{remaining})
        |sql}]

  let get_invite =
    [%rapper
      get_opt
        {sql| SELECT @string{code}, @string{did}, @int{remaining}
              FROM invite_codes WHERE code = %string{code}
        |sql}
        record_out]

  let use_invite =
    [%rapper
      get_opt
        {sql| UPDATE invite_codes SET remaining = remaining - 1
              WHERE code = %string{code} AND remaining > 0
              RETURNING @int{remaining}
        |sql}]

  let list_invites =
    [%rapper
      get_many
        {sql| SELECT @string{code}, @string{did}, @int{remaining}
              FROM invite_codes
              ORDER BY code ASC
              LIMIT %int{limit}
        |sql}
        record_out]

  let delete_invite =
    [%rapper
      execute
        {sql| DELETE FROM invite_codes WHERE code = %string{code}
        |sql}]

  let update_invite =
    [%rapper
      execute
        {sql| UPDATE invite_codes SET did = %string{did}, remaining = %int{remaining}
              WHERE code = %string{code}
        |sql}]

  let list_actors_filtered =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @int?{email_confirmed_at}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}, @string?{auth_code}, @int?{auth_code_expires_at}, @string?{pending_email}, COALESCE(@int{email_2fa_enabled}, 0), @Blob?{totp_secret}, @int?{totp_verified_at}
              FROM actors
              WHERE (did LIKE '%' || %string{filter} || '%'
                  OR handle LIKE '%' || %string{filter} || '%'
                  OR email LIKE '%' || %string{filter} || '%')
              AND did > %string{cursor}
              ORDER BY did ASC LIMIT %int{limit}
        |sql}
        record_out]

  let list_all_actors =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @int?{email_confirmed_at}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}, @string?{auth_code}, @int?{auth_code_expires_at}, @string?{pending_email}, COALESCE(@int{email_2fa_enabled}, 0), @Blob?{totp_secret}, @int?{totp_verified_at}
              FROM actors
              WHERE did > %string{cursor}
              ORDER BY did ASC LIMIT %int{limit}
        |sql}
        record_out]

  (* reserved keys *)
  let create_reserved_key =
    [%rapper
      execute
        {sql| INSERT INTO reserved_keys (key_did, did, private_key, created_at)
              VALUES (%string{key_did}, %string?{did}, %string{private_key}, %int{created_at})
        |sql}]

  let get_reserved_key_by_did did =
    [%rapper
      get_opt
        {sql| SELECT @string{key_did}, @string?{did}, @string{private_key}, @int{created_at}
              FROM reserved_keys WHERE did = %string{did}
        |sql}
        record_out]
      did

  let get_reserved_key key_did =
    [%rapper
      get_opt
        {sql| SELECT @string{key_did}, @string?{did}, @string{private_key}, @int{created_at}
              FROM reserved_keys WHERE key_did = %string{key_did}
        |sql}
        record_out]
      key_did

  let delete_reserved_key =
    [%rapper
      execute
        {sql| DELETE FROM reserved_keys WHERE key_did = %string{key_did}
        |sql}]

  let delete_reserved_keys_by_did =
    [%rapper
      execute
        {sql| DELETE FROM reserved_keys WHERE did = %string{did}
        |sql}]

  (* 2fa *)
  let set_auth_code =
    [%rapper
      execute
        {sql| UPDATE actors SET auth_code = %string{code}, auth_code_expires_at = %int{expires_at}
              WHERE did = %string{did}
        |sql}]

  let set_pending_email =
    [%rapper
      execute
        {sql| UPDATE actors SET auth_code = %string{code}, auth_code_expires_at = %int{expires_at}, pending_email = %string{pending_email}
              WHERE did = %string{did}
        |sql}]

  let clear_auth_code =
    [%rapper
      execute
        {sql| UPDATE actors SET auth_code = NULL, auth_code_expires_at = NULL, pending_email = NULL
              WHERE did = %string{did}
        |sql}]

  let get_actor_by_auth_code code =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @int?{email_confirmed_at}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}, @string?{auth_code}, @int?{auth_code_expires_at}, @string?{pending_email}, COALESCE(@int{email_2fa_enabled}, 0), @Blob?{totp_secret}, @int?{totp_verified_at}
              FROM actors WHERE auth_code = %string{code}
              LIMIT 1
        |sql}
        record_out]
      code

  let update_password =
    [%rapper
      execute
        {sql| UPDATE actors SET password_hash = %string{password_hash}, auth_code = NULL, auth_code_expires_at = NULL
              WHERE did = %string{did}
        |sql}]

  let update_email =
    [%rapper
      execute
        {sql| UPDATE actors SET email = %string{email}, email_confirmed_at = NULL, auth_code = NULL, auth_code_expires_at = NULL, pending_email = NULL
              WHERE did = %string{did}
        |sql}]

  let confirm_email =
    [%rapper
      execute
        {sql| UPDATE actors SET email_confirmed_at = %int{confirmed_at}, auth_code = NULL, auth_code_expires_at = NULL
              WHERE did = %string{did}
        |sql}]

  (* firehose *)
  let firehose_insert =
    [%rapper
      get_one
        {sql| INSERT INTO firehose (time, t, data)
              VALUES (%int{time}, %string{t}, %Blob{data})
              RETURNING @int{seq}
        |sql}]

  let firehose_since =
    [%rapper
      get_many
        {sql| SELECT @int{seq}, @int{time}, @string{t}, @Blob{data}
              FROM firehose
              WHERE seq > %int{since}
              ORDER BY seq ASC
              LIMIT %int{limit}
        |sql}
        record_out]

  let firehose_next =
    [%rapper
      get_opt
        {sql| SELECT @int{seq}, @int{time}, @string{t}, @Blob{data}
              FROM firehose
              WHERE seq > %int{cursor}
              ORDER BY seq ASC
              LIMIT 1
        |sql}
        record_out]

  let firehose_earliest_after =
    [%rapper
      get_opt
        {sql| SELECT @int{seq}, @int{time}, @string{t}, @Blob{data}
              FROM firehose
              WHERE time >= %int{time}
              ORDER BY time ASC
              LIMIT 1
        |sql}
        record_out]

  let firehose_latest_seq =
    [%rapper get_one {sql| SELECT MAX(seq) AS @int?{seq} FROM firehose |sql}] ()

  let get_revoked_token =
    [%rapper
      get_opt
        {sql| SELECT @int{revoked_at} FROM revoked_tokens WHERE did = %string{did} AND jti = %string{jti} |sql}]

  let revoke_token =
    [%rapper
      execute
        {sql| INSERT INTO revoked_tokens (did, jti, revoked_at) VALUES (%string{did}, %string{jti}, %int{now}) |sql}]
end

type t = Util.Sqlite.caqti_pool

let pool : t option ref = ref None

let pool_mutex = Lwt_mutex.create ()

let connect ?create () : t Lwt.t =
  match !pool with
  | Some pool ->
      Lwt.return pool
  | None ->
      Lwt_mutex.with_lock pool_mutex (fun () ->
          (* pool might've been initialized while we were waiting for the mutex *)
          match !pool with
          | Some pool ->
              Lwt.return pool
          | None ->
              if create = Some true then
                Util.mkfile_p Util.Constants.pegasus_db_filepath ~perm:0o644 ;
              let%lwt db =
                Util.Sqlite.connect ?create ~write:true
                  Util.Constants.pegasus_db_location
              in
              let%lwt () = Migrations.run_migrations Data_store db in
              pool := Some db ;
              Lwt.return db )

let connect_readonly ?create () =
  if create = Some true then
    Util.mkfile_p Util.Constants.pegasus_db_filepath ~perm:0o644 ;
  let%lwt db =
    Util.Sqlite.connect ?create ~write:false Util.Constants.pegasus_db_location
  in
  let%lwt () = Migrations.run_migrations Data_store db in
  Lwt.return db

let create_actor ~did ~handle ~email ~password ~signing_key conn =
  let password_hash = Bcrypt.hash password |> Bcrypt.string_of_hash in
  let now = Util.Time.now_ms () in
  Util.Sqlite.use_pool conn
  @@ Queries.create_actor ~did ~handle ~email ~password_hash ~signing_key
       ~created_at:now
       ~preferences:(Yojson.Safe.from_string "[]")

let get_actor_by_identifier id conn =
  Util.Sqlite.use_pool conn @@ Queries.get_actor_by_identifier ~id

let activate_actor did conn = Util.Sqlite.use_pool conn @@ Queries.activate_actor ~did

let deactivate_actor did conn =
  let deactivated_at = Util.Time.now_ms () in
  Util.Sqlite.use_pool conn @@ Queries.deactivate_actor ~did ~deactivated_at

let delete_actor did conn = Util.Sqlite.use_pool conn @@ Queries.delete_actor ~did

let update_actor_handle ~did ~handle conn =
  Util.Sqlite.use_pool conn @@ Queries.update_actor_handle ~did ~handle

let try_login ~id ~password conn =
  match%lwt get_actor_by_identifier id conn with
  | None ->
      Lwt.return_none
  | Some actor -> (
      let password_hash = actor.password_hash |> Bcrypt.hash_of_string in
      match Bcrypt.verify password password_hash with
      | true ->
          Lwt.return_some actor
      | _ ->
          Lwt.return_none )

let list_actors ?(cursor = "") ?(limit = 100) conn =
  Util.Sqlite.use_pool conn @@ Queries.list_actors ~cursor ~limit

let put_preferences ~did ~prefs conn =
  Util.Sqlite.use_pool conn @@ Queries.put_preferences ~did ~preferences:prefs

(* invite codes *)
let create_invite ~code ~did ~remaining conn =
  Util.Sqlite.use_pool conn @@ Queries.create_invite ~code ~did ~remaining

let get_invite ~code conn = Util.Sqlite.use_pool conn @@ Queries.get_invite ~code

let use_invite ~code conn = Util.Sqlite.use_pool conn @@ Queries.use_invite ~code

let list_invites ?(limit = 100) conn =
  Util.Sqlite.use_pool conn @@ Queries.list_invites ~limit

let delete_invite ~code conn = Util.Sqlite.use_pool conn @@ Queries.delete_invite ~code

let update_invite ~code ~did ~remaining conn =
  Util.Sqlite.use_pool conn @@ Queries.update_invite ~code ~did ~remaining

let list_actors_filtered ?(cursor = "") ?(limit = 100) ~filter conn =
  if String.length filter = 0 then
    Util.Sqlite.use_pool conn @@ Queries.list_all_actors ~cursor ~limit
  else Util.Sqlite.use_pool conn @@ Queries.list_actors_filtered ~filter ~cursor ~limit

(* reserved keys *)
let create_reserved_key ~key_did ~did ~private_key conn =
  let created_at = Util.Time.now_ms () in
  Util.Sqlite.use_pool conn
  @@ Queries.create_reserved_key ~key_did ~did ~private_key ~created_at

let get_reserved_key_by_did ~did conn =
  Util.Sqlite.use_pool conn @@ Queries.get_reserved_key_by_did ~did

let get_reserved_key ~key_did conn =
  Util.Sqlite.use_pool conn @@ Queries.get_reserved_key ~key_did

let delete_reserved_key ~key_did conn =
  Util.Sqlite.use_pool conn @@ Queries.delete_reserved_key ~key_did

let delete_reserved_keys_by_did ~did conn =
  Util.Sqlite.use_pool conn @@ Queries.delete_reserved_keys_by_did ~did

(* 2fa *)
let set_auth_code ~did ~code ~expires_at conn =
  Util.Sqlite.use_pool conn @@ Queries.set_auth_code ~did ~code ~expires_at

let set_pending_email ~did ~code ~expires_at ~pending_email conn =
  Util.Sqlite.use_pool conn
  @@ Queries.set_pending_email ~did ~code ~expires_at ~pending_email

let clear_auth_code ~did conn =
  Util.Sqlite.use_pool conn @@ Queries.clear_auth_code ~did

let get_actor_by_auth_code ~code conn =
  Util.Sqlite.use_pool conn @@ Queries.get_actor_by_auth_code ~code

let update_password ~did ~password conn =
  let password_hash = Bcrypt.hash password |> Bcrypt.string_of_hash in
  Util.Sqlite.use_pool conn @@ Queries.update_password ~did ~password_hash

let update_email ~did ~email conn =
  Util.Sqlite.use_pool conn @@ Queries.update_email ~did ~email

let confirm_email ~did conn =
  let confirmed_at = Util.Time.now_ms () in
  Util.Sqlite.use_pool conn @@ Queries.confirm_email ~did ~confirmed_at

(* firehose helpers *)
let append_firehose_event conn ~time ~t ~data : int Lwt.t =
  Util.Sqlite.use_pool conn @@ Queries.firehose_insert ~time ~t ~data

let list_firehose_since conn ~since ~limit : firehose_event list Lwt.t =
  Util.Sqlite.use_pool conn @@ Queries.firehose_since ~since ~limit

let next_firehose_event conn ~cursor : firehose_event option Lwt.t =
  Util.Sqlite.use_pool conn @@ Queries.firehose_next ~cursor

let earliest_firehose_after_time conn ~time : firehose_event option Lwt.t =
  Util.Sqlite.use_pool conn @@ Queries.firehose_earliest_after ~time

let latest_firehose_seq conn : int option Lwt.t =
  Util.Sqlite.use_pool conn @@ Queries.firehose_latest_seq

let next_firehose_seq conn : int Lwt.t =
  let%lwt seq = Util.Sqlite.use_pool conn Queries.firehose_latest_seq in
  Option.map succ seq |> Option.value ~default:0 |> Lwt.return

(* jwts *)
let is_token_revoked conn ~did ~jti =
  Util.Sqlite.use_pool conn @@ Queries.get_revoked_token ~did ~jti

let revoke_token conn ~did ~jti =
  Util.Sqlite.use_pool conn @@ Queries.revoke_token ~did ~jti ~now:(Util.Time.now_ms ())
