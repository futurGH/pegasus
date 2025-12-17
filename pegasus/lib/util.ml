module Constants = struct
  let data_dir =
    Core.Filename.to_absolute_exn Env.data_dir
      ~relative_to:(Core_unix.getcwd ())

  let pegasus_db_filepath = Filename.concat data_dir "pegasus.db"

  let pegasus_db_location = "sqlite3://" ^ pegasus_db_filepath |> Uri.of_string

  let user_db_filepath did =
    let dirname = Filename.concat data_dir "store" in
    let filename = Str.global_replace (Str.regexp ":") "_" did in
    Filename.concat dirname filename ^ ".db"

  let user_db_location did =
    "sqlite3://" ^ user_db_filepath did |> Uri.of_string

  let user_blobs_location did =
    did
    |> Str.global_replace (Str.regexp ":") "_"
    |> (Filename.concat data_dir "blobs" |> Filename.concat)
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

module Did_doc_types = struct
  type string_or_null = string option

  let string_or_null_to_yojson = function Some s -> `String s | None -> `Null

  let string_or_null_of_yojson = function
    | `String s ->
        Ok (Some s)
    | `Null ->
        Ok None
    | _ ->
        Error "invalid field value"

  type string_or_strings = [`String of string | `Strings of string list]

  let string_or_strings_to_yojson = function
    | `String c ->
        `String c
    | `Strings cs ->
        `List (List.map (fun c -> `String c) cs)

  let string_or_strings_of_yojson = function
    | `String c ->
        Ok (`Strings [c])
    | `List cs ->
        Ok (`Strings (Yojson.Safe.Util.filter_string cs))
    | _ ->
        Error "invalid field value"

  type string_map = (string * string) list

  let string_map_to_yojson = function
    | [] ->
        `Assoc []
    | m ->
        `Assoc (List.map (fun (k, v) -> (k, `String v)) m)

  let string_map_of_yojson = function
    | `Null ->
        Ok []
    | `Assoc m ->
        Ok
          (List.filter_map
             (fun (k, v) ->
               match (k, v) with _, `String s -> Some (k, s) | _, _ -> None )
             m )
    | _ ->
        Error "invalid field value"

  type string_or_string_map = [`String of string | `String_map of string_map]

  let string_or_string_map_to_yojson = function
    | `String c ->
        `String c
    | `String_map m ->
        `Assoc (List.map (fun (k, v) -> (k, `String v)) m)

  let string_or_string_map_of_yojson = function
    | `String c ->
        Ok (`String c)
    | `Assoc m ->
        string_map_of_yojson (`Assoc m) |> Result.map (fun m -> `String_map m)
    | _ ->
        Error "invalid field value"

  type string_or_string_map_or_either_list =
    [ `String of string
    | `String_map of string_map
    | `List of string_or_string_map list ]

  let string_or_string_map_or_either_list_to_yojson = function
    | `String c ->
        `String c
    | `String_map m ->
        `Assoc (List.map (fun (k, v) -> (k, `String v)) m)
    | `List l ->
        `List (List.map string_or_string_map_to_yojson l)

  let string_or_string_map_or_either_list_of_yojson = function
    | `String c ->
        Ok (`String c)
    | `Assoc m ->
        string_map_of_yojson (`Assoc m) |> Result.map (fun m -> `String_map m)
    | `List l ->
        Ok
          (`List
             ( List.map string_or_string_map_of_yojson l
             |> List.filter_map (function Ok x -> Some x | Error _ -> None) ) )
    | _ ->
        Error "invalid field value"
end

type caqti_pool = (Caqti_lwt.connection, Caqti_error.t) Caqti_lwt_unix.Pool.t

(* turns a caqti error into an exception *)
let caqti_result_exn = function
  | Ok x ->
      Ok x
  | Error caqti_err ->
      Error (Caqti_error.Exn caqti_err)

let _init_connection conn =
  match%lwt
    [%rapper
      execute
        {sql|
          PRAGMA journal_mode=WAL;
          PRAGMA foreign_keys=ON;
          PRAGMA synchronous=NORMAL;
          PRAGMA busy_timeout=5000;
        |sql}
        syntax_off]
      () conn
  with
  | Ok conn ->
      Lwt.return conn
  | Error e ->
      raise (Caqti_error.Exn e)

(* creates an sqlite pool *)
let connect_sqlite ?(create = false) ?(write = true) db_uri : caqti_pool Lwt.t =
  let uri =
    Uri.add_query_params' db_uri
      [("create", string_of_bool create); ("write", string_of_bool write)]
  in
  let pool_config = Caqti_pool_config.create ~max_size:16 ~max_idle_size:4 () in
  match
    Caqti_lwt_unix.connect_pool ~pool_config
      ~post_connect:(fun conn -> Lwt_result.ok @@ _init_connection conn)
      uri
  with
  | Ok pool ->
      Lwt.return pool
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

(* runs a bunch of queries and catches duplicate insertion, returning how many succeeded *)
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
          Lwt.return result ) )

(* unix timestamp *)
let now_ms () : int = int_of_float (Unix.gettimeofday () *. 1000.)

let ms_to_iso8601 ms =
  let s = float_of_int ms /. 1000. in
  Timedesc.(of_timestamp_float_s_exn s |> to_iso8601)

(* returns all blob refs in a record *)
let rec find_blob_refs (record : Mist.Lex.repo_record) : Mist.Blob_ref.t list =
  List.fold_left
    (fun acc (_, value) ->
      match value with
      | `BlobRef blob ->
          blob :: acc
      | `LexMap map ->
          find_blob_refs map @ acc
      | _ ->
          acc )
    []
    (Mist.Lex.String_map.bindings record)

let validate_handle handle =
  let front =
    String.sub handle 0 (String.length handle - (String.length Env.hostname + 1))
  in
  if String.contains front '.' then Error "handle can't contain periods"
  else
    match String.length front with
    | l when l < 3 ->
        Error "handle too short"
    | l when l > 18 ->
        Error "handle too long"
    | _ ->
        Ok ()

let mkfile_p path ~perm =
  Core_unix.mkdir_p (Filename.dirname path) ~perm:0o755 ;
  Core_unix.openfile ~mode:[O_CREAT; O_WRONLY] ~perm path |> Core_unix.close

let sig_matches_some_did_key ~did_keys ~signature ~msg =
  List.find_opt
    (fun key ->
      let raw, (module Curve) =
        Kleidos.parse_multikey_str (String.sub key 8 (String.length key - 8))
      in
      let valid =
        Curve.verify ~pubkey:(Curve.normalize_pubkey_to_raw raw) ~signature ~msg
      in
      valid )
    did_keys
  <> None

let rec http_get ?(max_redirects = 5) ?headers uri =
  let%lwt ans = Cohttp_lwt_unix.Client.get ?headers uri in
  follow_redirect ~max_redirects uri ans

and follow_redirect ~max_redirects request_uri (response, body) =
  let status = Http.Response.status response in
  (* the unconsumed body would otherwise leak memory *)
  let%lwt () =
    if status <> `OK then Cohttp_lwt.Body.drain_body body else Lwt.return_unit
  in
  match status with
  | `Permanent_redirect | `Moved_permanently ->
      handle_redirect ~permanent:true ~max_redirects request_uri response
  | `Found | `Temporary_redirect ->
      handle_redirect ~permanent:false ~max_redirects request_uri response
  | _ ->
      Lwt.return (response, body)

and handle_redirect ~permanent ~max_redirects request_uri response =
  if max_redirects <= 0 then failwith "too many redirects"
  else
    let headers = Http.Response.headers response in
    let location = Http.Header.get headers "location" in
    match location with
    | None ->
        failwith "redirection without Location header"
    | Some url ->
        let uri = Uri.of_string url in
        let%lwt () =
          if permanent then
            Logs_lwt.warn (fun m ->
                m "Permanent redirection from %s to %s"
                  (Uri.to_string request_uri)
                  url )
          else Lwt.return_unit
        in
        http_get uri ~max_redirects:(max_redirects - 1)

let copy_query req = Dream.all_queries req |> List.map (fun (k, v) -> (k, [v]))

let make_headers headers =
  List.fold_left
    (fun headers (k, v) ->
      match v with
      | Some value ->
          Http.Header.add headers k value
      | None ->
          headers )
    (Http.Header.init ()) headers

module type Template = sig
  type props

  val props_of_json : Yojson.Basic.t -> props

  val props_to_json : props -> Yojson.Basic.t

  val make : ?key:string -> props:props -> unit -> React.element
end

let render_html ?status ?title (type props)
    (template : (module Template with type props = props)) ~props =
  let module Template = (val template : Template with type props = props) in
  let props_json = Template.props_to_json props |> Yojson.Basic.to_string in
  let page_data = Printf.sprintf "window.__PAGE__ = {props: %s};" props_json in
  let app = Template.make ~props () in
  let page = Frontend.Layout.make ?title ~children:app () in
  Dream.stream ?status
    ~headers:[("Content-Type", "text/html")]
    (fun stream ->
      [%lwt
        let html, subscribe =
          ReactServerDOM.render_html ~skipRoot:false
            ~bootstrapScriptContent:page_data
            ~bootstrapScripts:["/public/client.js"] page
        in
        [%lwt
          let () = Dream.write stream html in
          [%lwt
            let () = Dream.flush stream in
            [%lwt
              let () =
                subscribe (fun chunk ->
                    [%lwt
                      let () = Dream.write stream chunk in
                      Dream.flush stream] )
              in
              Dream.flush stream]]]] )

let make_data_uri ~mimetype ~data =
  let base64_data = data |> Bytes.to_string |> Base64.encode_string in
  Printf.sprintf "data:%s;base64,%s" mimetype base64_data

let at_uri_regexp =
  Re.Pcre.re
    {|^at:\/\/([a-zA-Z0-9._:%-]+)(?:\/([a-zA-Z0-9-.]+)(?:\/([a-zA-Z0-9._~:@!$&%')(*+,;=-]+))?)?(?:#(\/[a-zA-Z0-9._~:@!$&%')(*+,;=\-[\]\/\\]*))?$|}
  |> Re.compile

type at_uri =
  {repo: string; collection: string; rkey: string; fragment: string option}

let parse_at_uri uri =
  match Re.exec_opt at_uri_regexp uri with
  | None ->
      None
  | Some m -> (
    try
      Some
        { repo= Re.Group.get m 1
        ; collection= Re.Group.get m 2
        ; rkey= Re.Group.get m 3
        ; fragment= Re.Group.get_opt m 4 }
    with _ -> None )

let make_at_uri ~repo ~collection ~rkey ~fragment =
  Printf.sprintf "at://%s/%s/%s%s" repo collection rkey
    (Option.value ~default:"" fragment)
