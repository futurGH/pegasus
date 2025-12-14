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
    ; auth_code_expires_at: int option }

  type invite_code = {code: string; did: string; remaining: int}

  type firehose_event = {seq: int; time: int; t: string; data: bytes}
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
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @int?{email_confirmed_at}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}, @string?{auth_code}, @int?{auth_code_expires_at}
              FROM actors WHERE did = %string{id} OR handle = %string{id} OR email = %string{id}
              LIMIT 1
        |sql}
        record_out]
      id

  let update_actor_handle =
    [%rapper
      execute
        {sql| UPDATE actors SET handle = %string{handle} WHERE did = %string{did}
        |sql}]

  let list_actors =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @int?{email_confirmed_at}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}, @string?{auth_code}, @int?{auth_code_expires_at}
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

  (* 2fa *)
  let set_auth_code =
    [%rapper
      execute
        {sql| UPDATE actors SET auth_code = %string{code}, auth_code_expires_at = %int{expires_at}
              WHERE did = %string{did}
        |sql}]

  let clear_auth_code =
    [%rapper
      execute
        {sql| UPDATE actors SET auth_code = NULL, auth_code_expires_at = NULL
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

type t = Util.caqti_pool

let connect ?create ?write () : t Lwt.t =
  if create = Some true then
    Util.mkfile_p Util.Constants.pegasus_db_filepath ~perm:0o644 ;
  Util.connect_sqlite ?create ?write Util.Constants.pegasus_db_location

let init conn : unit Lwt.t = Migrations.run_migrations conn

let create_actor ~did ~handle ~email ~password ~signing_key conn =
  let password_hash = Bcrypt.hash password |> Bcrypt.string_of_hash in
  let now = Util.now_ms () in
  Util.use_pool conn
  @@ Queries.create_actor ~did ~handle ~email ~password_hash ~signing_key
       ~created_at:now
       ~preferences:(Yojson.Safe.from_string "{}")

let get_actor_by_identifier id conn =
  Util.use_pool conn @@ Queries.get_actor_by_identifier ~id

let update_actor_handle ~did ~handle conn =
  Util.use_pool conn @@ Queries.update_actor_handle ~did ~handle

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
  Util.use_pool conn @@ Queries.list_actors ~cursor ~limit

let put_preferences ~did ~prefs conn =
  Util.use_pool conn @@ Queries.put_preferences ~did ~preferences:prefs

(* invite codes *)
let create_invite ~code ~did ~remaining conn =
  Util.use_pool conn @@ Queries.create_invite ~code ~did ~remaining

let get_invite ~code conn = Util.use_pool conn @@ Queries.get_invite ~code

let use_invite ~code conn = Util.use_pool conn @@ Queries.use_invite ~code

(* 2fa *)
let set_auth_code ~did ~code ~expires_at conn =
  Util.use_pool conn @@ Queries.set_auth_code ~did ~code ~expires_at

let clear_auth_code ~did conn =
  Util.use_pool conn @@ Queries.clear_auth_code ~did

(* firehose helpers *)
let append_firehose_event conn ~time ~t ~data : int Lwt.t =
  Util.use_pool conn @@ Queries.firehose_insert ~time ~t ~data

let list_firehose_since conn ~since ~limit : firehose_event list Lwt.t =
  Util.use_pool conn @@ Queries.firehose_since ~since ~limit

let next_firehose_event conn ~cursor : firehose_event option Lwt.t =
  Util.use_pool conn @@ Queries.firehose_next ~cursor

let earliest_firehose_after_time conn ~time : firehose_event option Lwt.t =
  Util.use_pool conn @@ Queries.firehose_earliest_after ~time

let latest_firehose_seq conn : int option Lwt.t =
  Util.use_pool conn @@ Queries.firehose_latest_seq

let next_firehose_seq conn : int Lwt.t =
  let%lwt seq = Util.use_pool conn Queries.firehose_latest_seq in
  Option.map succ seq |> Option.value ~default:0 |> Lwt.return

(* jwts *)
let is_token_revoked conn ~did ~jti =
  Util.use_pool conn @@ Queries.get_revoked_token ~did ~jti

let revoke_token conn ~did ~jti =
  Util.use_pool conn @@ Queries.revoke_token ~did ~jti ~now:(Util.now_ms ())
