module Constants = struct
  let pegasus_db_location =
    Filename.concat Env.database_dir "sqlite3://pegasus.db" |> Uri.of_string

  let user_db_location did =
    did
    |> Str.global_replace (Str.regexp ":") "_"
    |> Format.sprintf "sqlite3://%s.db"
    |> Filename.concat Env.database_dir
    |> Uri.of_string
end

module Syntax = struct
  let unwrap m =
    match%lwt m with
    | Ok x ->
        Lwt.return x
    | Error e ->
        raise (Caqti_error.Exn e)

  (* unwraps an Lwt result, raising an exception if there's an error *)
  let ( let$! ) m f =
    match%lwt m with Ok x -> f x | Error e -> raise (Caqti_error.Exn e)

  (* unwraps an Lwt result, raising an exception if there's an error *)
  let ( >$! ) m f =
    match%lwt m with
    | Ok x ->
        Lwt.return (f x)
    | Error e ->
        raise (Caqti_error.Exn e)
end

module Rapper = struct
  module CID : Rapper.CUSTOM with type t = Cid.t = struct
    type t = Cid.t

    let t =
      let encode cid =
        try Ok (Cid.to_string cid) with e -> Error (Printexc.to_string e)
      in
      Caqti_type.(custom ~encode ~decode:Cid.of_string string)
  end

  module Blob : Rapper.CUSTOM with type t = bytes = struct
    type t = bytes

    let t =
      let encode blob =
        try Ok (Bytes.to_string blob) with e -> Error (Printexc.to_string e)
      in
      let decode blob =
        try Ok (Bytes.of_string blob) with e -> Error (Printexc.to_string e)
      in
      Caqti_type.(custom ~encode ~decode string)
  end

  module Json : Rapper.CUSTOM with type t = Yojson.Safe.t = struct
    type t = Yojson.Safe.t

    let t =
      let encode json =
        try Ok (Yojson.Safe.to_string json ~std:true)
        with e -> Error (Printexc.to_string e)
      in
      let decode json =
        try Ok (Yojson.Safe.from_string json)
        with e -> Error (Printexc.to_string e)
      in
      Caqti_type.(custom ~encode ~decode string)
  end
end

(* turns a caqti error into an exception *)
let caqti_result_exn = function
  | Ok x ->
      Ok x
  | Error caqti_err ->
      Error (Caqti_error.Exn caqti_err)

let _init_connection conn =
  let open Syntax in
  let$! () =
    [%rapper
      execute
        {sql|
          PRAGMA journal_mode=WAL;
          PRAGMA foreign_keys=ON;
          PRAGMA synchronous=NORMAL;
        |sql}
        syntax_off]
      () conn
  in
  Lwt.return conn

(* opens an sqlite connection *)
let connect_sqlite db_uri =
  match%lwt Caqti_lwt_unix.connect db_uri with
  | Ok c ->
      _init_connection c
  | Error e ->
      raise (Caqti_error.Exn e)

let with_connection db_uri f =
  match%lwt
    Caqti_lwt_unix.with_connection db_uri (fun conn ->
        let%lwt _ = _init_connection conn in
        f conn )
  with
  | Ok result ->
      Lwt.return result
  | Error e ->
      raise (Caqti_error.Exn e)

(* runs a bunch of queries and catches duplicate insertion, returning how many succeeded *)
let multi_query connection
    (queries : (unit -> ('a, Caqti_error.t) Lwt_result.t) list) :
    (int, exn) Lwt_result.t =
  let open Syntax in
  let module C = (val connection : Caqti_lwt.CONNECTION) in
  let$! () = C.start () in
  let is_ignorable_error e =
    match (e : Caqti_error.t) with
    | `Request_failed qe | `Response_failed qe -> (
      match Caqti_error.cause (`Request_failed qe) with
      | `Not_null_violation | `Unique_violation ->
          true
      | _ ->
          false )
    | _ ->
        false
  in
  let rec aux acc queries =
    match acc with
    | Error e ->
        Lwt.return_error e
    | Ok count -> (
      match queries with
      | [] ->
          Lwt.return (Ok count)
      | query :: rest -> (
          let%lwt result = query () in
          match result with
          | Ok _ ->
              aux (Ok (count + 1)) rest
          | Error e ->
              if is_ignorable_error e then aux (Ok count) rest
              else Lwt.return_error (Caqti_error.Exn e) ) )
  in
  aux (Ok 0) queries

(* unix timestamp *)
let now_ms () : int = int_of_float (Unix.gettimeofday () *. 1000.)

let ms_to_iso8601 ms =
  let s = float_of_int ms /. 1000. in
  Timedesc.(of_timestamp_float_s_exn s |> to_iso8601)

(* returns all blob refs in a record *)
let find_blob_refs (record : Mist.Lex.repo_record) : Mist.Blob_ref.t list =
  List.fold_left
    (fun acc (_, value) ->
      match value with `BlobRef blob -> blob :: acc | _ -> acc )
    []
    (Mist.Lex.StringMap.bindings record)

(* returns whether the value is None *)
let is_none = function None -> true | _ -> false
