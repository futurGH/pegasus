open Cohttp_lwt
open Cohttp_lwt_unix

type init = Auth.Verifiers.ctx

type context = {req: Dream.request; db: Data_store.t; auth: Auth.credentials}

type handler = context -> Dream.response Lwt.t

let handler ?(auth : Auth.Verifiers.t = Any) (hdlr : handler) (init : init) =
  let open Errors in
  let auth = Auth.Verifiers.of_t auth in
  try%lwt
    match%lwt auth init with
    | Ok creds -> (
      try%lwt hdlr {req= init.req; db= init.db; auth= creds}
      with e ->
        if not (is_xrpc_error e) then log_exn ~req:init.req e ;
        exn_to_response e )
    | Error e ->
        exn_to_response e
  with e ->
    if not (is_xrpc_error e) then log_exn ~req:init.req e ;
    exn_to_response e

let parse_query (req : Dream.request)
    (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a =
  try
    let queries = Dream.all_queries req in
    let query_json = `Assoc (List.map (fun (k, v) -> (k, `String v)) queries) in
    query_json |> of_yojson |> Result.get_ok
  with _ -> Errors.invalid_request "invalid query string"

let parse_body (req : Dream.request)
    (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a Lwt.t =
  try%lwt
    let%lwt body = Dream.body req in
    body |> Yojson.Safe.from_string |> of_yojson |> Result.get_ok |> Lwt.return
  with _ -> Errors.invalid_request "invalid request body"

let parse_proxy_header req =
  match Dream.header req "atproto-proxy" with
  | Some header -> (
    match String.split_on_char '#' header with
    | [did; typ] ->
        Some (did, typ)
    | _ ->
        None )
  | None ->
      None

let nsid_regex =
  Re.Pcre.re
    {|^[a-zA-Z](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+\.[a-zA-Z][a-zA-Z0-9]{0,62}?$|}
  |> Re.compile

let service_proxy ?lxm ?aud (ctx : context) =
  let did = Auth.get_authed_did_exn ctx.auth in
  let nsid = (Dream.path [@warning "-3"]) ctx.req |> List.rev |> List.hd in
  if Re.exec_opt nsid_regex nsid = None then
    Errors.invalid_request ("invalid nsid " ^ nsid) ;
  let service_did, service_type =
    match parse_proxy_header ctx.req with
    | Some (did, typ) ->
        (did, typ)
    | None ->
        Errors.invalid_request "invalid proxy header"
  in
  let aud = Option.value aud ~default:service_did in
  let lxm = Option.value lxm ~default:nsid in
  Auth.assert_rpc_scope ctx.auth ~aud ~lxm ;
  let fragment = "#" ^ service_type in
  match%lwt Id_resolver.Did.resolve service_did with
  | Ok did_doc -> (
      let scheme, host =
        match Id_resolver.Did.Document.get_service did_doc fragment with
        | Some service -> (
            let svc_uri = Uri.of_string service in
            match (Uri.scheme svc_uri, Uri.host svc_uri) with
            | Some scheme, Some host when scheme = "http" || scheme = "https" ->
                (scheme, host)
            | _ ->
                Errors.invalid_request ("failed to parse service URL " ^ service)
            )
        | None ->
            Errors.invalid_request
              ("failed to resolve destination service for " ^ fragment)
      in
      let%lwt signing_multikey =
        match%lwt Data_store.get_actor_by_identifier did ctx.db with
        | Some {signing_key; _} ->
            Lwt.return signing_key
        | None ->
            Errors.internal_error ~msg:"user not found" ()
      in
      let signing_key = Kleidos.parse_multikey_str signing_multikey in
      let jwt = Jwt.generate_service_jwt ~did ~aud ~lxm ~signing_key in
      let uri = Uri.make ~scheme ~host ~path:(Dream.target ctx.req) () in
      let headers =
        Util.make_headers
          [ ("accept-language", Dream.header ctx.req "accept-language")
          ; ( "atproto-accept-labelers"
            , Dream.header ctx.req "atproto-accept-labelers" )
          ; ("authorization", Some ("Bearer " ^ jwt)) ]
      in
      match Dream.method_ ctx.req with
      | `GET -> (
          let%lwt res, body = Util.http_get uri ~headers in
          match res.status with
          | `OK ->
              Dream.stream ~status:`OK (fun stream ->
                  Body.to_stream body |> Lwt_stream.iter_s (Dream.write stream) )
          | e ->
              let%lwt () = Body.drain_body body in
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
              Dream.stream ~status:`OK (fun stream ->
                  Body.to_stream body |> Lwt_stream.iter_s (Dream.write stream) )
          | e ->
              let%lwt () = Body.drain_body body in
              Dream.error (fun log ->
                  log "error when proxying to %s: %s" (Uri.to_string uri)
                    (Http.Status.to_string e) ) ;
              Errors.internal_error ~msg:"failed to proxy request" () )
      | _ ->
          Errors.invalid_request "unsupported method" )
  | Error e ->
      Dream.error (fun log ->
          log ~request:ctx.req "error when resolving destination service: %s" e ) ;
      Errors.internal_error ~msg:"failed to resolve destination service" ()

let service_proxy_handler db req =
  match Dream.header req "atproto-proxy" with
  | Some _ ->
      handler ~auth:Authorization service_proxy {req; db}
  | None ->
      Dream.empty `Not_Found

let dpop_middleware inner_handler req =
  let%lwt res = inner_handler req in
  match Dream.header req "DPoP" with
  | Some _ ->
      Dream.add_header res "DPoP-Nonce" (Oauth.Dpop.next_nonce ()) ;
      Dream.add_header res "Access-Control-Expose-Headers" "DPoP-Nonce" ;
      Lwt.return res
  | None ->
      Lwt.return res

let cors_middleware inner_handler req =
  let%lwt res = inner_handler req in
  let origin = Dream.header req "Origin" in
  Dream.add_header res "Access-Control-Allow-Origin"
    (Option.value origin ~default:"*") ;
  Dream.add_header res "Access-Control-Allow-Methods"
    "GET, POST, PUT, DELETE, OPTIONS" ;
  Dream.add_header res "Access-Control-Allow-Headers" "*" ;
  Dream.add_header res "Access-Control-Max-Age" "86400" ;
  Lwt.return res

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
