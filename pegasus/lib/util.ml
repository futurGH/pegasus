module Syntax = struct
  let ( let$! ) m f =
    match%lwt m with Ok x -> f x | Error e -> raise (Caqti_error.Exn e)
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
