type storage = Local | S3

let storage_of_string = function
  | "local" ->
      Local
  | "s3" ->
      S3
  | s ->
      failwith ("unknown storage type: " ^ s)

let storage_to_string = function Local -> "local" | S3 -> "s3"

let s3_key ~did ~cid = Printf.sprintf "blobs/%s/%s" did cid

let local_path ~did ~cid =
  Filename.concat (Util.Constants.user_blobs_location did) cid

let cdn_redirect_url ~did ~cid : string option =
  match Env.s3_config with
  | Some {cdn_url= Some cdn_url; blobs_enabled= true; _} ->
      Some (Printf.sprintf "%s/%s" cdn_url (s3_key ~did ~cid))
  | _ ->
      None

let put_s3 ~did ~cid ~data : unit Lwt.t =
  match Env.s3_config with
  | Some config when config.blobs_enabled -> (
      let key = s3_key ~did ~cid in
      let%lwt result =
        Aws_s3_lwt.S3.put ~credentials:config.credentials_obj
          ~endpoint:config.endpoint_obj ~bucket:config.bucket ~key
          ~data:(Bytes.to_string data) ()
      in
      match result with
      | Ok _ ->
          Lwt.return_unit
      | Error e ->
          failwith
            (Printf.sprintf "S3 put failed: %s" (Util.s3_error_to_string e)) )
  | _ ->
      failwith "S3 not configured for blob storage"

let get_s3 ~did ~cid : bytes option Lwt.t =
  match Env.s3_config with
  | Some config -> (
      let key = s3_key ~did ~cid in
      let%lwt result =
        Aws_s3_lwt.S3.get ~credentials:config.credentials_obj
          ~endpoint:config.endpoint_obj ~bucket:config.bucket ~key ()
      in
      match result with
      | Ok data ->
          Lwt.return_some (Bytes.of_string data)
      | Error _ ->
          Lwt.return_none )
  | None ->
      Lwt.return_none

let delete_s3 ~did ~cid : unit Lwt.t =
  match Env.s3_config with
  | Some config -> (
      let key = s3_key ~did ~cid in
      let%lwt result =
        Aws_s3_lwt.S3.delete ~credentials:config.credentials_obj
          ~endpoint:config.endpoint_obj ~bucket:config.bucket ~key ()
      in
      match result with
      | Ok () ->
          Lwt.return_unit
      | Error e ->
          Dream.error (fun log ->
              log "S3 delete failed for %s: %s" key (Util.s3_error_to_string e) ) ;
          Lwt.return_unit )
  | None ->
      Lwt.return_unit

let put_local ~did ~cid ~data : unit =
  let file = local_path ~did ~cid in
  Core_unix.mkdir_p (Filename.dirname file) ~perm:0o755 ;
  Out_channel.with_open_bin file (fun oc -> Out_channel.output_bytes oc data)

let get_local ~did ~cid : bytes option =
  let file = local_path ~did ~cid in
  if Sys.file_exists file then
    Some (In_channel.with_open_bin file In_channel.input_all |> Bytes.of_string)
  else None

let delete_local ~did ~cid : unit =
  let file = local_path ~did ~cid in
  if Sys.file_exists file then Sys.remove file

let put ~did ~cid ~data : storage Lwt.t =
  match Env.s3_config with
  | Some {blobs_enabled= true; _} ->
      let%lwt () = put_s3 ~did ~cid:(Cid.to_string cid) ~data in
      Lwt.return S3
  | _ ->
      put_local ~did ~cid:(Cid.to_string cid) ~data ;
      Lwt.return Local

let get ~did ~cid ~storage : bytes option Lwt.t =
  let cid_str = Cid.to_string cid in
  match storage with
  | Local ->
      Lwt.return (get_local ~did ~cid:cid_str)
  | S3 ->
      get_s3 ~did ~cid:cid_str

let delete ~did ~cid ~storage : unit Lwt.t =
  let cid_str = Cid.to_string cid in
  match storage with
  | Local ->
      delete_local ~did ~cid:cid_str ;
      Lwt.return_unit
  | S3 ->
      delete_s3 ~did ~cid:cid_str
