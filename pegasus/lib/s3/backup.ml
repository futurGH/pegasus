let timestamp_string () =
  let tm = Unix.gmtime (Unix.gettimeofday ()) in
  Printf.sprintf "%04d-%02d-%02d_%02d-%02d-%02d" (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1) tm.Unix.tm_mday tm.Unix.tm_hour tm.Unix.tm_min
    tm.Unix.tm_sec

let upload_to_s3 ~(config : Env.s3_config) ~file_path ~key : unit Lwt.t =
  let%lwt data =
    Lwt_io.with_file ~mode:Lwt_io.Input file_path (fun ic -> Lwt_io.read ic)
  in
  let%lwt result =
    Aws_s3_lwt.S3.put ~credentials:config.credentials_obj
      ~endpoint:config.endpoint_obj ~bucket:config.bucket ~key ~data ()
  in
  match result with
  | Ok _ ->
      Log.info (fun log -> log "S3 backup uploaded: %s" key) ;
      Lwt.return_unit
  | Error e ->
      Log.err (fun log ->
          log "S3 backup upload failed for %s: %s" key
            (Util.s3_error_to_string e) ) ;
      Lwt.return_unit

let backup_main_db ~config : unit Lwt.t =
  let db_path = Util.Constants.pegasus_db_filepath in
  if Sys.file_exists db_path then (
    let timestamp = timestamp_string () in
    let key = Printf.sprintf "backups/pegasus-%s.db" timestamp in
    Log.info (fun log -> log "starting main database backup") ;
    upload_to_s3 ~config ~file_path:db_path ~key )
  else (
    Log.warn (fun log -> log "main database not found: %s" db_path) ;
    Lwt.return_unit )

let backup_user_db ~config ~did : unit Lwt.t =
  let db_path = Util.Constants.user_db_filepath did in
  if Sys.file_exists db_path then
    let timestamp = timestamp_string () in
    let did_safe = Str.global_replace (Str.regexp ":") "_" did in
    let key = Printf.sprintf "backups/store/%s-%s.db" did_safe timestamp in
    upload_to_s3 ~config ~file_path:db_path ~key
  else Lwt.return_unit

let backup_all_user_dbs ~config ~ds : unit Lwt.t =
  Log.info (fun log -> log "starting backup of user databases") ;
  let rec backup_batch cursor count =
    let%lwt actors = Data_store.list_actors ~cursor ~limit:100 ds in
    match actors with
    | [] ->
        Log.info (fun log -> log "backed up %d user databases" count) ;
        Lwt.return_unit
    | actors ->
        let%lwt () =
          Lwt_list.iter_s
            (fun (actor : Data_store.Types.actor) ->
              backup_user_db ~config ~did:actor.did )
            actors
        in
        let last = List.hd (List.rev actors) in
        backup_batch last.did (count + List.length actors)
  in
  backup_batch "" 0

let do_backup () : unit Lwt.t =
  match Env.s3_config with
  | Some config when config.backups_enabled ->
      Log.info (fun log -> log "starting S3 backup") ;
      let%lwt ds = Data_store.connect () in
      let%lwt () = backup_main_db ~config in
      let%lwt () = backup_all_user_dbs ~config ~ds in
      Log.info (fun log -> log "S3 backup completed") ;
      Lwt.return_unit
  | _ ->
      Lwt.return_unit

let queue_backups () : unit Lwt.t =
  match Env.s3_config with
  | Some config when config.backups_enabled ->
      Log.info (fun log -> log "starting hourly S3 backups") ;
      let%lwt () =
        Lwt.catch
          (fun () -> do_backup ())
          (fun e ->
            Log.err (fun log -> log "backup failed: %s" (Printexc.to_string e)) ;
            Lwt.return_unit )
      in
      let rec loop () =
        let%lwt () = Lwt_unix.sleep config.backup_interval_s in
        let%lwt () =
          Lwt.catch
            (fun () -> do_backup ())
            (fun e ->
              Log.err (fun log ->
                  log "backup failed: %s" (Printexc.to_string e) ) ;
              Lwt.return_unit )
        in
        loop ()
      in
      loop ()
  | _ ->
      Lwt.return_unit

let start () : unit = Lwt.async queue_backups
