open Util.Rapper
open Util.Syntax

module Types = struct
  type actor =
    { id: int
    ; did: string
    ; handle: string
    ; email: string
    ; password_hash: bytes
    ; signing_key: bytes
    ; preferences: Yojson.Safe.t
    ; created_at: int
    ; deactivated_at: int option }
end

open Types

module Queries = struct
  let create_tables =
    [%rapper
      execute
        {sql| CREATE TABLE IF NOT EXISTS actors (
                id INTEGER PRIMARY KEY,
                did TEXT NOT NULL UNIQUE,
                handle TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                password_hash BLOB NOT NULL,
                signing_key BLOB NOT NULL,
                preferences TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                deactivated_at INTEGER
              );
              CREATE INDEX IF NOT EXISTS actors_did_idx ON actors (did);
              CREATE INDEX IF NOT EXISTS actors_handle_idx ON actors (handle);
              CREATE INDEX IF NOT EXISTS actors_email_idx ON actors (email);
        |sql}]
      ()

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
                %Blob{password_hash},
                %Blob{signing_key},
                %Json{preferences},
                %int{created_at}
              );
        |sql}]

  let get_actor_by_identifier id =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @Blob{password_hash}, @Blob{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}
              FROM actors WHERE did = %string{id} OR handle = %string{id} OR email = %string{id}
              LIMIT 1
        |sql}
        record_out]
      id

  let list_actors =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{did}, @string{handle}, @string{email}, @Blob{password_hash}, @Blob{signing_key}, @Json{preferences}, @int{created_at}, @int?{deactivated_at}
              FROM actors
              ORDER BY created_at DESC LIMIT %int{limit} OFFSET %int{offset}
        |sql}
        record_out]
end

type t = (module Rapper_helper.CONNECTION)

let init conn : unit Lwt.t =
  let$! () = Queries.create_tables conn in
  Lwt.return_unit

let create_actor ~did ~handle ~email ~password ~signing_key conn =
  let password_hash =
    Bcrypt.hash password |> Bcrypt.string_of_hash |> Bytes.of_string
  in
  let now = Unix.gettimeofday () *. 1000. |> int_of_float in
  let$! () =
    Queries.create_actor ~did ~handle ~email ~password_hash ~signing_key
      ~created_at:now
      ~preferences:(Yojson.Safe.from_string "{}")
      conn
  in
  Lwt.return_unit

let get_actor_by_identifier id conn =
  let$! actor = Queries.get_actor_by_identifier ~id conn in
  Lwt.return actor

let list_actors ?(limit = 100) ?(offset = 0) conn =
  let$! actors = Queries.list_actors ~limit ~offset conn in
  Lwt.return actors
