[@@@ocaml.warning "-33"]

open Lwt.Infix

type migration = {id: int; name: string; applied_at: int}

module Queries = struct
  open Util.Rapper
  open Util.Syntax

  let create_migrations_table =
    [%rapper
      execute
        {sql| CREATE TABLE IF NOT EXISTS schema_migrations (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at INTEGER NOT NULL
              )
        |sql}]
      ()

  let get_applied_migrations =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{name}, @int{applied_at}
              FROM schema_migrations
              ORDER BY id ASC
        |sql}
        record_out]
      ()

  let record_migration =
    [%rapper
      execute
        {sql| INSERT INTO schema_migrations (id, name, applied_at)
              VALUES (%int{id}, %string{name}, %int{applied_at})
        |sql}]
end

let execute_raw db_path sql =
  let db = Sqlite3.db_open db_path in
  try
    let rc = Sqlite3.exec db sql in
    let _ = try Sqlite3.db_close db with _ -> true in
    match rc with
    | Sqlite3.Rc.OK ->
        Lwt.return_ok ()
    | _ ->
        let err_msg = Sqlite3.errmsg db in
        Lwt.return_error (Failure ("sql error: " ^ err_msg))
  with e ->
    let _ = try Sqlite3.db_close db with _ -> true in
    Lwt.return_error e

let parse_migration_filename filename =
  try
    let regex = Str.regexp "^\\([0-9]+\\)_\\(.*\\)\\.sql$" in
    if Str.string_match regex filename 0 then
      let id = Str.matched_group 1 filename |> int_of_string in
      let name = Str.matched_group 2 filename in
      Some (id, name, filename)
    else None
  with _ -> None

let read_migration_files migrations_dir =
  try
    let files = Sys.readdir migrations_dir |> Array.to_list in
    let migrations =
      files
      |> List.filter_map (fun filename ->
          match parse_migration_filename filename with
          | Some (id, name, _) ->
              let full_path = Filename.concat migrations_dir filename in
              Some (id, name, full_path)
          | None ->
              None )
      |> List.sort (fun (id1, _, _) (id2, _, _) -> compare id1 id2)
    in
    Lwt.return migrations
  with Sys_error _ -> Lwt.return []

let run_migration conn (id, name, filepath) =
  let%lwt () = Lwt_io.printlf "running migration %03d: %s" id name in
  let%lwt sql_content =
    Lwt_io.with_file ~mode:Lwt_io.Input filepath (fun ic -> Lwt_io.read ic)
  in
  let%lwt result = execute_raw Util.Constants.pegasus_db_filepath sql_content in
  let%lwt () =
    match result with Ok () -> Lwt.return_unit | Error e -> raise e
  in
  let applied_at = Util.now_ms () in
  let%lwt () =
    Util.use_pool conn (Queries.record_migration ~id ~name ~applied_at)
  in
  Lwt_io.printlf "migration %03d applied successfully" id

let run_migrations ?(migrations_dir = "migrations") conn =
  let%lwt () = Util.use_pool conn Queries.create_migrations_table in
  let%lwt applied =
    Util.use_pool conn Queries.get_applied_migrations
    >|= List.map (fun m -> m.id)
  in
  let%lwt available = read_migration_files migrations_dir in
  let pending =
    List.filter (fun (id, _, _) -> not (List.mem id applied)) available
  in
  match pending with
  | [] ->
      Lwt_io.printl "no pending migrations"
  | _ ->
      let%lwt () =
        Lwt_io.printlf "found %d pending migrations" (List.length pending)
      in
      Lwt_list.iter_s (run_migration conn) pending
