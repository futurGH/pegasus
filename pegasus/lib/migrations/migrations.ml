open Lwt.Infix

type migration = {id: int; name: string; applied_at: int}

module Queries = struct
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

let execute_raw conn sql =
  let module C = (val conn : Caqti_lwt.CONNECTION) in
  match C.driver_connection with
  | Some (Caqti_driver_sqlite3.Driver_connection db) -> (
    match Sqlite3.exec db sql with
    | Sqlite3.Rc.OK ->
        Lwt.return_ok ()
    | _ ->
        let err_msg = Sqlite3.errmsg db in
        Lwt.return_error
          (Caqti_error.request_failed
             ~uri:(Uri.of_string "sqlite3://")
             ~query:sql (Caqti_error.Msg err_msg) ) )
  | _ ->
      Lwt.return_error
        (Caqti_error.request_failed
           ~uri:(Uri.of_string "sqlite3://")
           ~query:sql (Caqti_error.Msg "driver_connection not available") )

let parse_migration_filename filename =
  try
    let regex = Str.regexp "^\\([0-9]+\\)_\\(.*\\)\\.sql$" in
    if Str.string_match regex filename 0 then
      let id = Str.matched_group 1 filename |> int_of_string in
      let name = Str.matched_group 2 filename in
      Some (id, name)
    else None
  with _ -> None

let run_migration db (id, name, sql) =
  Util.use_pool db (fun conn ->
      Util.transact conn (fun () ->
          let open Lwt_result.Infix in
          execute_raw conn sql
          >>= fun () ->
          let applied_at = Util.now_ms () in
          Queries.record_migration ~id ~name ~applied_at conn ) )

type migration_type = Data_store | User_store

let run_migrations typ conn =
  let read_migration, file_list =
    match typ with
    | Data_store ->
        Data_store_migrations_sql.(read, file_list)
    | User_store ->
        User_store_migrations_sql.(read, file_list)
  in
  let%lwt () = Util.use_pool conn Queries.create_migrations_table in
  let%lwt applied =
    Util.use_pool conn Queries.get_applied_migrations
    >|= List.map (fun m -> m.id)
  in
  let pending =
    List.filter_map
      (fun filename ->
        match parse_migration_filename filename with
        | Some (id, name) when not (List.mem id applied) -> begin
          match read_migration filename with
          | Some sql ->
              Some (id, name, sql)
          | None ->
              None
          end
        | _ ->
            None )
      file_list
  in
  match pending with
  | [] ->
      Lwt.return_unit
  | _ -> (
    try%lwt Lwt_list.iter_s (run_migration conn) pending with
    | Caqti_error.Exn e ->
        failwith ("failed to run migrations: " ^ Caqti_error.show e)
    | exn ->
        failwith ("failed to run migrations: " ^ Printexc.to_string exn) )
