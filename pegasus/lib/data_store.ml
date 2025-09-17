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
              );
              CREATE INDEX IF NOT EXISTS actors_did_idx ON actors (did);
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
            );
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
            );
        |sql}
          syntax_off]
        () conn
    in
    [%rapper
      execute
        (* no need to store issued tokens, just revoked ones; stolen from millipds https://github.com/DavidBuchanan314/millipds/blob/8f89a01e7d367a2a46f379960e9ca50347dcce71/src/millipds/database.py#L253 *)
        {sql| CREATE TABLE IF NOT EXISTS revoked_tokens (
            did TEXT NOT NULL,
            jti TEXT NOT NULL,
            revoked_at INTEGER NOT NULL,
            PRIMARY KEY (did, jti)
          );
       |sql}]
      () conn

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
              );
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

type t = (module Rapper_helper.CONNECTION)

let connect ?create ?write () : t Lwt.t =
  Util.connect_sqlite ?create ?write Util.Constants.pegasus_db_location

let init conn : unit Lwt.t = unwrap @@ Queries.create_tables conn

let create_actor ~did ~handle ~email ~password ~signing_key conn =
  let password_hash = Bcrypt.hash password |> Bcrypt.string_of_hash in
  let now = Util.now_ms () in
  let$! () =
    Queries.create_actor ~did ~handle ~email ~password_hash ~signing_key
      ~created_at:now
      ~preferences:(Yojson.Safe.from_string "{}")
      conn
  in
  Lwt.return_unit

let get_actor_by_identifier id conn =
  unwrap @@ Queries.get_actor_by_identifier ~id conn

let update_actor_handle ~did ~handle conn =
  unwrap @@ Queries.update_actor_handle ~did ~handle conn

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
  unwrap @@ Queries.list_actors ~cursor ~limit conn

let put_preferences ~did ~prefs conn =
  unwrap @@ Queries.put_preferences ~did ~preferences:prefs conn

(* firehose helpers *)
let append_firehose_event conn ~time ~t ~data : int Lwt.t =
  unwrap @@ Queries.firehose_insert ~time ~t ~data conn

let list_firehose_since conn ~since ~limit : firehose_event list Lwt.t =
  unwrap @@ Queries.firehose_since ~since ~limit conn

let next_firehose_event conn ~cursor : firehose_event option Lwt.t =
  unwrap @@ Queries.firehose_next ~cursor conn

let earliest_firehose_after_time conn ~time : firehose_event option Lwt.t =
  unwrap @@ Queries.firehose_earliest_after ~time conn

let latest_firehose_seq conn : int option Lwt.t =
  unwrap @@ Queries.firehose_latest_seq conn

let next_firehose_seq conn : int Lwt.t =
  Queries.firehose_latest_seq conn
  >$! fun s -> s |> Option.map succ |> Option.value ~default:0

(* jwts *)
let is_token_revoked conn ~did ~jti =
  unwrap @@ Queries.get_revoked_token conn ~did ~jti

let revoke_token conn ~did ~jti =
  unwrap @@ Queries.revoke_token conn ~did ~jti ~now:(Util.now_ms ())
