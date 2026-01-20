open Lexicons.Com.Atproto.Server.DeleteAccount.Main

let rec rm_rf path =
  if Sys.is_directory path then (
    Sys.readdir path
    |> Array.iter (fun name -> rm_rf (Filename.concat path name)) ;
    Sys.rmdir path )
  else Sys.remove path

let delete_account ~did db =
  let%lwt () =
    try%lwt
      Util.Sqlite.use_pool db (fun conn ->
          Util.Sqlite.transact conn (fun () ->
              let open Util.Syntax in
              let$! () =
                Data_store.Queries.delete_reserved_keys_by_did ~did conn
              in
              let$! () = Data_store.Queries.delete_actor ~did conn in
              let user_db_file = Util.Constants.user_db_filepath did in
              let user_blobs_dir = Util.Constants.user_blobs_location did in
              ( if Sys.file_exists user_db_file then
                  try Sys.remove user_db_file with _ -> () ) ;
              ( if Sys.file_exists user_blobs_dir then
                  try rm_rf user_blobs_dir with _ -> () ) ;
              Lwt.return_ok () ) )
    with e ->
      Errors.(
        log_exn e ;
        internal_error ~msg:"failed to delete account" () )
  in
  Sequencer.sequence_account db ~did ~active:false ~status:`Deleted ()

let handler =
  Xrpc.handler (fun {req; db; _} ->
      let%lwt {did; password; token} = Xrpc.parse_body req input_of_yojson in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.invalid_request "account not found"
      | Some actor -> (
          let password_hash = actor.password_hash |> Bcrypt.hash_of_string in
          if not (Bcrypt.verify password password_hash) then
            Errors.auth_required "invalid did or password" ;
          match (actor.auth_code, actor.auth_code_expires_at) with
          | Some auth_code, Some auth_expires_at
            when String.starts_with ~prefix:"del-" auth_code
                 && token = auth_code
                 && Util.Time.now_ms () < auth_expires_at ->
              let%lwt _ = delete_account ~did db in
              Dream.empty `OK
          | None, _ | _, None ->
              Errors.invalid_request ~name:"InvalidToken" "token is invalid"
          | _ ->
              Errors.invalid_request ~name:"ExpiredToken" "token is expired" ) )
