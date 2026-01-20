type caqti_pool = (Caqti_lwt.connection, Caqti_error.t) Caqti_lwt_unix.Pool.t

(* turns a caqti error into an exception *)
let caqti_result_exn = function
  | Ok x ->
      Ok x
  | Error caqti_err ->
      Error (Caqti_error.Exn caqti_err)

let _init_connection (module Db : Rapper_helper.CONNECTION) :
    (unit, Caqti_error.t) Lwt_result.t =
  let open Lwt_result.Syntax in
  let open Caqti_request.Infix in
  let open Caqti_type in
  let* _ =
    Db.find (((unit ->! string) ~oneshot:true) "PRAGMA journal_mode=WAL") ()
  in
  let* _ =
    Db.exec (((unit ->. unit) ~oneshot:true) "PRAGMA foreign_keys=ON") ()
  in
  let* _ =
    Db.exec (((unit ->. unit) ~oneshot:true) "PRAGMA synchronous=NORMAL") ()
  in
  let* _ =
    Db.find (((unit ->! int) ~oneshot:true) "PRAGMA busy_timeout=5000") ()
  in
  Lwt.return_ok ()

(* creates an sqlite pool *)
let connect ?(create = false) ?(write = true) db_uri : caqti_pool Lwt.t =
  let uri =
    Uri.add_query_params' db_uri
      [("create", string_of_bool create); ("write", string_of_bool write)]
  in
  let pool_config = Caqti_pool_config.create ~max_size:16 ~max_idle_size:4 () in
  match
    Caqti_lwt_unix.connect_pool ~pool_config ~post_connect:_init_connection uri
  with
  | Ok pool ->
      Lwt.return pool
  | Error e ->
      raise (Caqti_error.Exn e)

let with_connection db_uri f =
  match%lwt
    Caqti_lwt_unix.with_connection db_uri (fun conn ->
        match%lwt _init_connection conn with
        | Ok () ->
            f conn
        | Error e ->
            Lwt.return_error e )
  with
  | Ok result ->
      Lwt.return result
  | Error e ->
      raise (Caqti_error.Exn e)

let use_pool ?(timeout = 60.0) pool
    (f : Caqti_lwt.connection -> ('a, Caqti_error.t) Lwt_result.t) : 'a Lwt.t =
  match%lwt
    Lwt_unix.with_timeout timeout (fun () -> Caqti_lwt_unix.Pool.use f pool)
  with
  | Ok res ->
      Lwt.return res
  | Error e ->
      raise (Caqti_error.Exn e)

let transact conn fn : (unit, 'e) Lwt_result.t =
  let module C = (val conn : Caqti_lwt.CONNECTION) in
  match%lwt C.start () with
  | Ok () -> (
    try%lwt
      match%lwt fn () with
      | Ok _ -> (
        match%lwt C.commit () with
        | Ok () ->
            Lwt.return_ok ()
        | Error e -> (
          match%lwt C.rollback () with
          | Ok () ->
              Lwt.return_error e
          | Error e ->
              Lwt.return_error e ) )
      | Error e -> (
        match%lwt C.rollback () with
        | Ok () ->
            Lwt.return_error e
        | Error e ->
            Lwt.return_error e )
    with e -> (
      match%lwt C.rollback () with
      | Ok () ->
          Lwt.return_error
            ( match e with
            | Caqti_error.Exn e ->
                e
            | e ->
                Caqti_error.request_failed ~query:"unknown"
                  ~uri:(Uri.of_string "//unknown")
                  (Caqti_error.Msg (Printexc.to_string e)) )
      | Error e ->
          Lwt.return_error e ) )
  | Error e ->
      Lwt.return_error e

(* runs a bunch of queries in a transaction, catches duplicate insertion, returning how many succeeded *)
let multi_query pool
    (queries : (Caqti_lwt.connection -> ('a, Caqti_error.t) Lwt_result.t) list)
    : (int, exn) Lwt_result.t =
  let open Syntax in
  Lwt_result.catch (fun () ->
      use_pool pool (fun connection ->
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
                  let%lwt result = query connection in
                  match result with
                  | Ok _ ->
                      aux (Ok (count + 1)) rest
                  | Error e ->
                      if is_ignorable_error e then aux (Ok count) rest
                      else Lwt.return_error e ) )
          in
          let%lwt result = aux (Ok 0) queries in
          match result with
          | Ok count ->
              let$! () = C.commit () in
              Lwt.return_ok count
          | Error e ->
              let%lwt _ = C.rollback () in
              Lwt.return_error e ) )
