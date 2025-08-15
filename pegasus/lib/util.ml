module Syntax = struct
  let ( let$! ) m f =
    match%lwt m with Ok x -> f x | Error e -> raise (Caqti_error.Exn e)
end

module Rapper = struct
  module Cid : Rapper.CUSTOM with type t = Cid.t = struct
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
end

let connect_sqlite db_uri =
  let open Syntax in
  match%lwt Caqti_lwt.connect (Uri.of_string db_uri) with
  | Ok c ->
      let$! () =
        [%rapper execute {sql| PRAGMA journal_mode=WAL; |sql} syntax_off] () c
      in
      let$! () =
        [%rapper execute {sql| PRAGMA synchronous=NORMAL; |sql} syntax_off] () c
      in
      Lwt.return c
  | Error e ->
      raise (Caqti_error.Exn e)
