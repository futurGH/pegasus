open Util.Rapper
open Util.Syntax

module Types = struct
  type actor =
    { id: int
    ; did: string
    ; handle: string
    ; email: string
    ; password_hash: string
    ; signing_key: string
    ; preferences: Yojson.Safe.t
    ; created_at: int
    ; deactivated_at: int option }

  type invite_code = {code: string; did: string; remaining: int}

  type firehose_event = {seq: int; time: int; t: string; data: bytes}
end

open Types

module Queries = struct
  let create_tables conn =
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS actors (
                id INTEGER PRIMARY KEY,
                did TEXT NOT NULL UNIQUE,
                handle TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                signing_key TEXT NOT NULL,
                preferences TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                deactivated_at INTEGER
              )
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE INDEX IF NOT EXISTS actors_did_idx ON actors (did);
                CREATE INDEX IF NOT EXISTS actors_handle_idx ON actors (handle);
                CREATE INDEX IF NOT EXISTS actors_email_idx ON actors (email);
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS invite_codes (
                code TEXT PRIMARY KEY,
                did TEXT NOT NULL,
                remaining INTEGER NOT NULL
              )
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS firehose (
              	seq INTEGER PRIMARY KEY,
              	time INTEGER NOT NULL,
              	t TEXT NOT NULL,
              	data BLOB NOT NULL
              )
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          (* no need to store issued tokens, just revoked ones; stolen from millipds https://github.com/DavidBuchanan314/millipds/blob/8f89a01e7d367a2a46f379960e9ca50347dcce71/src/millipds/database.py#L253 *)
          {sql| CREATE TABLE IF NOT EXISTS revoked_tokens (
            	did TEXT NOT NULL,
            	jti TEXT NOT NULL,
            	revoked_at INTEGER NOT NULL,
            	PRIMARY KEY (did, jti)
              )
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS oauth_requests (
		        request_id TEXT PRIMARY KEY,
		        client_id TEXT NOT NULL,
		        request_data TEXT NOT NULL,
		        dpop_jkt TEXT,
		        expires_at INTEGER NOT NULL,
		        created_at INTEGER NOT NULL
		      )
		  |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS oauth_codes (
                code TEXT PRIMARY KEY,
                request_id TEXT NOT NULL REFERENCES oauth_requests(request_id),
                authorized_by TEXT,
                authorized_at INTEGER,
                expires_at INTEGER NOT NULL,
                used BOOLEAN DEFAULT FALSE
              )
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TABLE IF NOT EXISTS oauth_tokens (
            	refresh_token TEXT UNIQUE NOT NULL,
            	client_id TEXT NOT NULL,
            	did TEXT NOT NULL,
            	dpop_jkt TEXT,
            	scope TEXT NOT NULL,
            	expires_at INTEGER NOT NULL
              )
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE INDEX oauth_requests_expires_idx ON oauth_requests(expires_at);
                CREATE INDEX oauth_codes_expires_idx ON oauth_codes(expires_at);
                CREATE INDEX oauth_tokens_refresh_idx ON oauth_tokens(refresh_token);
          |sql}]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TRIGGER IF NOT EXISTS cleanup_expired_oauth_requests
                AFTER INSERT ON oauth_requests
                BEGIN
                  DELETE FROM oauth_requests WHERE expires_at < unixepoch() * 1000;
                END
          |sql}
          syntax_off]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TRIGGER IF NOT EXISTS cleanup_expired_oauth_codes
                AFTER INSERT ON oauth_codes
                BEGIN
                  DELETE FROM oauth_codes WHERE expires_at < unixepoch() * 1000 OR used = 1;
                END
          |sql}
          syntax_off]
        () conn
    in
    let$! () =
      [%rapper
        execute
          {sql| CREATE TRIGGER IF NOT EXISTS cleanup_expired_oauth_tokens
                AFTER INSERT ON oauth_tokens
                BEGIN
                  DELETE FROM oauth_tokens WHERE expires_at < unixepoch() * 1000;
                END
          |sql}
          syntax_off]
        () conn
    in
    Lwt.return_ok ()

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
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}
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
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @string{password_hash}, @string{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}
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
  Util.connect_sqlite ?create ?write Util.Constants.pegasus_db_location

let init conn : unit Lwt.t = Util.use_pool conn Queries.create_tables

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
