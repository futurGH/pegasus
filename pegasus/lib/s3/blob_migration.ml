let migrate_user ~did : (int * int) Lwt.t =
  match Env.s3_config with
  | Some config when config.blobs_enabled ->
      Log.info (fun log -> log "migrating blobs for user %s" did) ;
      let%lwt user_db = User_store.connect did in
      let migrated = ref 0 in
      let errors = ref 0 in
      let rec migrate_batch cursor =
        let%lwt blobs =
          User_store.list_blobs_by_storage user_db ~storage:Blob_store.Local
            ~limit:100 ~cursor
        in
        match blobs with
        | [] ->
            Lwt.return_unit
        | blobs ->
            let%lwt () =
              Lwt_list.iter_s
                (fun (cid, _mimetype) ->
                  Lwt.catch
                    (fun () ->
                      let local_path =
                        Blob_store.local_path ~did ~cid:(Cid.to_string cid)
                      in
                      if Sys.file_exists local_path then (
                        let data =
                          In_channel.with_open_bin local_path
                            In_channel.input_all
                          |> Bytes.of_string
                        in
                        let%lwt () =
                          Blob_store.put_s3 ~did ~cid:(Cid.to_string cid) ~data
                        in
                        let%lwt () =
                          User_store.update_blob_storage user_db cid
                            Blob_store.S3
                        in
                        Sys.remove local_path ; incr migrated ; Lwt.return_unit
                        )
                      else (
                        Log.warn (fun log ->
                            log "local blob file not found: %s" local_path ) ;
                        Lwt.return_unit ) )
                    (fun e ->
                      Log.err (fun log ->
                          log "blob migration error for %s: %s"
                            (Cid.to_string cid) (Printexc.to_string e) ) ;
                      incr errors ;
                      Lwt.return_unit ) )
                blobs
            in
            let last_cid, _ = List.hd (List.rev blobs) in
            migrate_batch (Cid.to_string last_cid)
      in
      let%lwt () = migrate_batch "" in
      Log.info (fun log ->
          log "blob migration complete for %s: %d migrated, %d errors" did
            !migrated !errors ) ;
      Lwt.return (!migrated, !errors)
  | _ ->
      Log.err (fun log -> log "S3 blob storage not enabled") ;
      Lwt.return (0, 0)

let migrate_all () : unit Lwt.t =
  match Env.s3_config with
  | Some config when config.blobs_enabled ->
      Log.info (fun log -> log "migrating all blobs to S3") ;
      let%lwt ds = Data_store.connect () in
      let total_migrated = ref 0 in
      let total_errors = ref 0 in
      let rec migrate_batch cursor =
        let%lwt actors = Data_store.list_actors ~cursor ~limit:100 ds in
        match actors with
        | [] ->
            Lwt.return_unit
        | actors ->
            let%lwt () =
              Lwt_list.iter_s
                (fun (actor : Data_store.Types.actor) ->
                  let%lwt migrated, errors = migrate_user ~did:actor.did in
                  total_migrated := !total_migrated + migrated ;
                  total_errors := !total_errors + errors ;
                  Lwt.return_unit )
                actors
            in
            let last = List.hd (List.rev actors) in
            migrate_batch last.did
      in
      let%lwt () = migrate_batch "" in
      Log.info (fun log ->
          log "blob migration complete: %d total migrated, %d total errors"
            !total_migrated !total_errors ) ;
      Lwt.return_unit
  | _ ->
      Log.err (fun log -> log "S3 blob storage not enabled") ;
      Lwt.return_unit
