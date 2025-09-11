open Cohttp_lwt
open Cohttp_lwt_unix

type init = Auth.Verifiers.ctx

type context = {req: Dream.request; db: Data_store.t; auth: Auth.credentials}

type handler = context -> Dream.response Lwt.t

let handler ?(auth : Auth.Verifiers.verifier = Auth.Verifiers.any)
    (hdlr : handler) (init : init) =
  let open Errors in
  match%lwt auth init with
  | Ok creds -> (
      try%lwt hdlr {req= init.req; db= init.db; auth= creds}
      with e ->
        ( match is_xrpc_error e with
        | true ->
            ()
        | false ->
            log_exn ~req:init.req e ) ;
        exn_to_response e )
  | Error e ->
      exn_to_response e

let parse_query (req : Dream.request)
    (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a =
  try
    let queries = Dream.all_queries req in
    let query_json =
      `Assoc (List.map (fun (k, v) -> (k, Yojson.Safe.from_string v)) queries)
    in
    query_json |> of_yojson |> Result.get_ok
  with _ -> Errors.invalid_request "Invalid query string"

let parse_body (req : Dream.request)
    (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a Lwt.t =
  try%lwt
    let%lwt body = Dream.body req in
    body |> Yojson.Safe.from_string |> of_yojson |> Result.get_ok |> Lwt.return
  with _ -> Errors.invalid_request "Invalid request body"

let service_proxy (ctx : context) (proxy_header : string) =
  let did = Auth.get_authed_did_exn ctx.auth in
  let nsid_regex =
    Str.regexp
      {|^[a-zA-Z](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+\.[a-zA-Z][a-zA-Z0-9]{0,62}?$|}
  in
  let nsid =
    (Dream.path [@warning "-3"]) ctx.req
    |> List.rev |> List.hd |> String.lowercase_ascii
  in
  if not (Str.string_match nsid_regex nsid 0) then
    Errors.invalid_request "invalid nsid" ;
  let service_did, service_type =
    match String.split_on_char '#' proxy_header with
    | [did; typ] ->
        (did, typ)
    | _ ->
        Errors.invalid_request "invalid proxy header"
  in
  let fragment = "#" ^ service_type in
  match%lwt Id_resolver.Did.resolve service_did with
  | Ok did_doc -> (
      let host =
        match Id_resolver.Did.Document.get_service did_doc fragment with
        | Some service ->
            service
        | None ->
            Errors.invalid_request "failed to resolve destination service"
      in
      let%lwt signing_key =
        match%lwt Data_store.get_actor_by_identifier did ctx.db with
        | Some {signing_key; _} ->
            Lwt.return signing_key
        | None ->
            Errors.internal_error ~msg:"user not found" ()
      in
      let jwt =
        Auth.generate_service_jwt ~did ~aud:service_did ~lxm:nsid ~signing_key
      in
      let uri =
        host ^ "/" ^ String.concat "/" @@ (Dream.path [@warning "-3"]) ctx.req
        |> Uri.of_string
      in
      let headers = Http.Header.of_list [("Authorization", "Bearer " ^ jwt)] in
      match Dream.method_ ctx.req with
      | `GET -> (
          let%lwt res, body = Client.get uri ~headers in
          match res.status with
          | `OK ->
              let%lwt body = Body.to_string body in
              Lwt.return @@ Dream.response ~status:`OK body
          | e ->
              Dream.error (fun log ->
                  log "error when proxying to %s: %s" (Uri.to_string uri)
                    (Http.Status.to_string e) ) ;
              Errors.internal_error ~msg:"failed to proxy request" () )
      | `POST -> (
          let%lwt req_body = Dream.body ctx.req in
          let%lwt res, body =
            Client.post uri ~headers ~body:(Body.of_string req_body)
          in
          match res.status with
          | `OK ->
              let%lwt body = Body.to_string body in
              Lwt.return @@ Dream.response ~status:`OK body
          | e ->
              Dream.error (fun log ->
                  log "error when proxying to %s: %s" (Uri.to_string uri)
                    (Http.Status.to_string e) ) ;
              Errors.internal_error ~msg:"failed to proxy request" () )
      | _ ->
          Errors.invalid_request "unsupported method" )
  | Error _ ->
      Errors.internal_error ~msg:"failed to resolve destination service" ()

let service_proxy_middleware db inner_handler req =
  match Dream.header req "atproto-proxy" with
  | Some header ->
      handler ~auth:Auth.Verifiers.access
        (fun ctx -> service_proxy ctx header)
        {req; db}
  | None ->
      inner_handler req

let resolve_repo_did ctx repo =
  if String.starts_with ~prefix:"did:" repo then Lwt.return repo
  else
    match%lwt Data_store.get_actor_by_identifier repo ctx.db with
    | Some {did; _} ->
        Lwt.return did
    | None ->
        Errors.invalid_request "target repository not found"

let resolve_repo_did_authed ctx repo =
  let%lwt input_did = resolve_repo_did ctx repo in
  let did =
    match ctx.auth with
    | Access {did} when did = input_did ->
        did
    | Admin ->
        input_did
    | _ ->
        Errors.auth_required "authentication does not match target repository"
  in
  Lwt.return did
