open Lwt.Syntax

type t =
  { service: Uri.t
  ; mutable headers: (string * string) list
  ; mutable session: Types.session option
  ; on_request: (t -> unit Lwt.t) option
        (* called before each request for token refresh *) }

module type S = sig
  val make : service:string -> unit -> t

  val make_with_interceptor :
    service:string -> on_request:(t -> unit Lwt.t) -> unit -> t

  val set_session : t -> Types.session -> unit

  val clear_session : t -> unit

  val get_session : t -> Types.session option

  val get_service : t -> Uri.t

  val query :
       t
    -> string
    -> Yojson.Safe.t
    -> (Yojson.Safe.t -> ('a, string) result)
    -> 'a Lwt.t

  val procedure :
       t
    -> string
    -> Yojson.Safe.t
    -> Yojson.Safe.t option
    -> (Yojson.Safe.t -> ('a, string) result)
    -> 'a Lwt.t

  val query_bytes : t -> string -> Yojson.Safe.t -> (string * string) Lwt.t

  val procedure_bytes :
       t
    -> string
    -> Yojson.Safe.t
    -> string option
    -> content_type:string
    -> (string * string) option Lwt.t

  val procedure_blob :
       t
    -> string
    -> Yojson.Safe.t
    -> bytes
    -> content_type:string
    -> (Yojson.Safe.t -> ('a, string) result)
    -> 'a Lwt.t
end

module Make (Http : Http_backend.S) : S = struct
  let make ~service () =
    let service = Uri.of_string service in
    {service; headers= []; session= None; on_request= None}

  let make_with_interceptor ~service ~on_request () =
    let service = Uri.of_string service in
    {service; headers= []; session= None; on_request= Some on_request}

  let set_session t session =
    t.session <- Some session ;
    t.headers <-
      List.filter (fun (k, _) -> k <> "Authorization") t.headers
      @ [("Authorization", "Bearer " ^ session.Types.access_jwt)]

  let clear_session t =
    t.session <- None ;
    t.headers <- List.filter (fun (k, _) -> k <> "Authorization") t.headers

  let get_session t = t.session

  let get_service t = t.service

  (* build query string from json params *)
  let params_to_query (params : Yojson.Safe.t) : (string * string list) list =
    match params with
    | `Assoc pairs ->
        List.filter_map
          (fun (k, v) ->
            match v with
            | `Null ->
                None
            | `Bool b ->
                Some (k, [string_of_bool b])
            | `Int i ->
                Some (k, [string_of_int i])
            | `Float f ->
                Some (k, [string_of_float f])
            | `String s ->
                Some (k, [s])
            | `List items ->
                let strs =
                  List.filter_map
                    (function
                      | `String s ->
                          Some s
                      | `Int i ->
                          Some (string_of_int i)
                      | `Bool b ->
                          Some (string_of_bool b)
                      | _ ->
                          None )
                    items
                in
                if strs = [] then None else Some (k, strs)
            | _ ->
                None )
          pairs
    | _ ->
        []

  let make_headers ?(extra = []) ?(accept = "application/json") t =
    Cohttp.Header.of_list
      ([("User-Agent", "hermes/1.0"); ("Accept", accept)] @ t.headers @ extra)

  let query (t : t) (nsid : string) (params : Yojson.Safe.t)
      (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a Lwt.t =
    (* call interceptor if present for token refresh *)
    let* () =
      match t.on_request with Some f -> f t | None -> Lwt.return_unit
    in
    let query = params_to_query params in
    let uri =
      Uri.with_path t.service ("/xrpc/" ^ nsid)
      |> fun u -> Uri.with_query u query
    in
    let headers = make_headers t in
    let* resp, body =
      Lwt.catch
        (fun () -> Lwt_unix.with_timeout 30.0 (fun () -> Http.get ~headers uri))
        (fun exn ->
          Types.raise_xrpc_error_raw ~status:0 ~error:"NetworkError"
            ~message:(Printexc.to_string exn) () )
    in
    let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    let* body_str = Cohttp_lwt.Body.to_string body in
    if status >= 200 && status < 300 then
      if String.length body_str = 0 then
        (* empty response, try parsing empty object *)
        match of_yojson (`Assoc []) with
        | Ok v ->
            Lwt.return v
        | Error e ->
            Types.raise_xrpc_error_raw ~status ~error:"ParseError" ~message:e ()
      else
        let json = Yojson.Safe.from_string body_str in
        match of_yojson json with
        | Ok v ->
            Lwt.return v
        | Error e ->
            Types.raise_xrpc_error_raw ~status ~error:"ParseError" ~message:e ()
    else
      let payload =
        try
          let json = Yojson.Safe.from_string body_str in
          match Types.xrpc_error_payload_of_yojson json with
          | Ok p ->
              p
          | Error _ ->
              {error= "UnknownError"; message= Some body_str}
        with _ -> {error= "UnknownError"; message= Some body_str}
      in
      Types.raise_xrpc_error ~status payload

  let procedure (t : t) (nsid : string) (params : Yojson.Safe.t)
      (input : Yojson.Safe.t option)
      (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a Lwt.t =
    (* call interceptor if present for token refresh *)
    let* () =
      match t.on_request with Some f -> f t | None -> Lwt.return_unit
    in
    let query = params_to_query params in
    let uri =
      Uri.with_path t.service ("/xrpc/" ^ nsid)
      |> fun u -> Uri.with_query u query
    in
    let body, content_type =
      match input with
      | Some json ->
          ( Cohttp_lwt.Body.of_string (Yojson.Safe.to_string json)
          , "application/json" )
      | None ->
          (Cohttp_lwt.Body.empty, "application/json")
    in
    let headers = make_headers ~extra:[("Content-Type", content_type)] t in
    let* resp, resp_body =
      Lwt.catch
        (fun () ->
          Lwt_unix.with_timeout 30.0 (fun () -> Http.post ~headers ~body uri) )
        (fun exn ->
          Types.raise_xrpc_error_raw ~status:0 ~error:"NetworkError"
            ~message:(Printexc.to_string exn) () )
    in
    let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    let* body_str = Cohttp_lwt.Body.to_string resp_body in
    if status >= 200 && status < 300 then
      if String.length body_str = 0 then
        match of_yojson (`Assoc []) with
        | Ok v ->
            Lwt.return v
        | Error e ->
            Types.raise_xrpc_error_raw ~status ~error:"ParseError" ~message:e ()
      else
        let json = Yojson.Safe.from_string body_str in
        match of_yojson json with
        | Ok v ->
            Lwt.return v
        | Error e ->
            Types.raise_xrpc_error_raw ~status ~error:"ParseError" ~message:e ()
    else
      let payload =
        try
          let json = Yojson.Safe.from_string body_str in
          match Types.xrpc_error_payload_of_yojson json with
          | Ok p ->
              p
          | Error _ ->
              {error= "UnknownError"; message= Some body_str}
        with _ -> {error= "UnknownError"; message= Some body_str}
      in
      Types.raise_xrpc_error ~status payload

  let query_bytes (t : t) (nsid : string) (params : Yojson.Safe.t) :
      (string * string) Lwt.t =
    (* call interceptor if present for token refresh *)
    let* () =
      match t.on_request with Some f -> f t | None -> Lwt.return_unit
    in
    let query = params_to_query params in
    let uri =
      Uri.with_path t.service ("/xrpc/" ^ nsid)
      |> fun u -> Uri.with_query u query
    in
    let headers = make_headers ~accept:"*/*" t in
    let* resp, body =
      Lwt.catch
        (fun () -> Lwt_unix.with_timeout 120.0 (fun () -> Http.get ~headers uri))
        (fun exn ->
          Types.raise_xrpc_error_raw ~status:0 ~error:"NetworkError"
            ~message:(Printexc.to_string exn) () )
    in
    let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    let* body_str = Cohttp_lwt.Body.to_string body in
    if status >= 200 && status < 300 then
      let content_type =
        Cohttp.Response.headers resp
        |> fun h ->
        Cohttp.Header.get h "content-type"
        |> Option.value ~default:"application/octet-stream"
      in
      Lwt.return (body_str, content_type)
    else
      let payload =
        try
          let json = Yojson.Safe.from_string body_str in
          match Types.xrpc_error_payload_of_yojson json with
          | Ok p ->
              p
          | Error _ ->
              {error= "UnknownError"; message= Some body_str}
        with _ -> {error= "UnknownError"; message= Some body_str}
      in
      Types.raise_xrpc_error ~status payload

  (* execute procedure with raw bytes input, returns raw bytes or none if no output *)
  let procedure_bytes (t : t) (nsid : string) (params : Yojson.Safe.t)
      (input : string option) ~(content_type : string) :
      (string * string) option Lwt.t =
    (* call interceptor if present for token refresh *)
    let* () =
      match t.on_request with Some f -> f t | None -> Lwt.return_unit
    in
    let query = params_to_query params in
    let uri =
      Uri.with_path t.service ("/xrpc/" ^ nsid)
      |> fun u -> Uri.with_query u query
    in
    let body =
      match input with
      | Some data ->
          Cohttp_lwt.Body.of_string data
      | None ->
          Cohttp_lwt.Body.empty
    in
    let headers =
      make_headers ~extra:[("Content-Type", content_type)] ~accept:"*/*" t
    in
    let* resp, resp_body =
      Lwt.catch
        (fun () ->
          Lwt_unix.with_timeout 120.0 (fun () -> Http.post ~headers ~body uri) )
        (fun exn ->
          Types.raise_xrpc_error_raw ~status:0 ~error:"NetworkError"
            ~message:(Printexc.to_string exn) () )
    in
    let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    let* body_str = Cohttp_lwt.Body.to_string resp_body in
    if status >= 200 && status < 300 then
      if String.length body_str = 0 then Lwt.return None
      else
        let resp_content_type =
          Cohttp.Response.headers resp
          |> fun h ->
          Cohttp.Header.get h "content-type"
          |> Option.value ~default:"application/octet-stream"
        in
        Lwt.return (Some (body_str, resp_content_type))
    else
      let payload =
        try
          let json = Yojson.Safe.from_string body_str in
          match Types.xrpc_error_payload_of_yojson json with
          | Ok p ->
              p
          | Error _ ->
              {error= "UnknownError"; message= Some body_str}
        with _ -> {error= "UnknownError"; message= Some body_str}
      in
      Types.raise_xrpc_error ~status payload

  let procedure_blob (t : t) (nsid : string) (params : Yojson.Safe.t)
      (blob_data : bytes) ~(content_type : string)
      (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a Lwt.t =
    (* call interceptor if present for token refresh *)
    let* () =
      match t.on_request with Some f -> f t | None -> Lwt.return_unit
    in
    let query = params_to_query params in
    let uri =
      Uri.with_path t.service ("/xrpc/" ^ nsid)
      |> fun u -> Uri.with_query u query
    in
    let body = Cohttp_lwt.Body.of_string (Bytes.to_string blob_data) in
    let headers = make_headers ~extra:[("Content-Type", content_type)] t in
    let* resp, resp_body =
      Lwt.catch
        (fun () ->
          Lwt_unix.with_timeout 120.0 (fun () -> Http.post ~headers ~body uri) )
        (fun exn ->
          Types.raise_xrpc_error_raw ~status:0 ~error:"NetworkError"
            ~message:(Printexc.to_string exn) () )
    in
    let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    let* body_str = Cohttp_lwt.Body.to_string resp_body in
    if status >= 200 && status < 300 then
      let json = Yojson.Safe.from_string body_str in
      match of_yojson json with
      | Ok v ->
          Lwt.return v
      | Error e ->
          Types.raise_xrpc_error_raw ~status ~error:"ParseError" ~message:e ()
    else
      let payload =
        try
          let json = Yojson.Safe.from_string body_str in
          match Types.xrpc_error_payload_of_yojson json with
          | Ok p ->
              p
          | Error _ ->
              {error= "UnknownError"; message= Some body_str}
        with _ -> {error= "UnknownError"; message= Some body_str}
      in
      Types.raise_xrpc_error ~status payload
end

(* default client using real http backend *)
include Make (Http_backend.Default)
