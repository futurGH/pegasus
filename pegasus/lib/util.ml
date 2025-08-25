module Exceptions = struct
  exception XrpcError of (string * string)
end

module Syntax = struct
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

(* opens an sqlite connection *)
let connect_sqlite db_uri =
  let open Syntax in
  match%lwt Caqti_lwt_unix.connect (Uri.of_string db_uri) with
  | Ok c ->
      let$! () =
        [%rapper execute {sql| PRAGMA journal_mode=WAL; |sql} syntax_off] () c
      in
      let$! () =
        [%rapper execute {sql| PRAGMA synchronous=NORMAL; |sql} syntax_off] () c
      in
      let$! () =
        [%rapper execute {sql| PRAGMA foreign_keys=ON; |sql} syntax_off] () c
      in
      Lwt.return c
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

(* returns all blob refs in a record *)
let find_blob_refs (record : Mist.Lex.repo_record) : Mist.Blob_ref.t list =
  List.fold_left
    (fun acc (_, value) ->
      match value with `BlobRef blob -> blob :: acc | _ -> acc )
    []
    (Mist.Lex.StringMap.bindings record)
