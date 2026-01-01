open Cohttp_lwt
open Cohttp_lwt_unix

type init = Auth.Verifiers.ctx

type context = {req: Dream.request; db: Data_store.t; auth: Auth.credentials}

type handler = context -> Dream.response Lwt.t

type rate_limit_rule =
  | Shared of
      { name: string
      ; calc_key: (context -> string option) option
      ; calc_points: (context -> int) option }
  | Route of
      { duration_ms: int
      ; points: int
      ; calc_key: (context -> string option) option
      ; calc_points: (context -> int) option }

let default_calc_key (ctx : context) = Some (Util.request_ip ctx.req)

let default_calc_points (_ctx : context) = 1

let consume_shared_rate_limit ~name ~key ~points =
  match Rate_limiter.Shared.get name with
  | Some limiter -> (
    match Rate_limiter.consume limiter ~key ~points with
    | Rate_limiter.Exceeded status ->
        raise (Rate_limiter.Rate_limit_exceeded status)
    | result ->
        result )
  | None ->
      failwith (Printf.sprintf "shared rate limiter %s not found" name)

let consume_route_rate_limit ~name ~duration_ms ~key ~max_points ~consume_points
    =
  let limiter =
    Rate_limiter.Route.get_or_create ~name ~duration_ms ~points:max_points
  in
  match Rate_limiter.consume limiter ~key ~points:consume_points with
  | Rate_limiter.Exceeded status ->
      raise (Rate_limiter.Rate_limit_exceeded status)
  | result ->
      result

let apply_rate_limits ~nsid (rules : rate_limit_rule list) (ctx : context) =
  let results =
    List.mapi
      (fun i rule ->
        match rule with
        | Shared {name; calc_key; calc_points} -> (
            let calc_key = Option.value calc_key ~default:default_calc_key in
            let calc_points =
              Option.value calc_points ~default:default_calc_points
            in
            match calc_key ctx with
            | None ->
                Rate_limiter.Skipped
            | Some key ->
                let points = calc_points ctx in
                consume_shared_rate_limit ~name ~key ~points )
        | Route {duration_ms; points= max_points; calc_key; calc_points} -> (
            let calc_key = Option.value calc_key ~default:default_calc_key in
            let calc_points =
              Option.value calc_points ~default:default_calc_points
            in
            let name = Printf.sprintf "%s-%d" nsid i in
            match calc_key ctx with
            | None ->
                Rate_limiter.Skipped
            | Some key ->
                let consume_points = calc_points ctx in
                consume_route_rate_limit ~name ~duration_ms ~key ~max_points
                  ~consume_points ) )
      rules
  in
  match Rate_limiter.get_tightest_limit results with
  | Some (Rate_limiter.Exceeded status) ->
      raise (Rate_limiter.Rate_limit_exceeded status)
  | _ ->
      ()

let rate_limit_response (status : Rate_limiter.status) =
  let reset_at =
    ((Unix.gettimeofday () *. 1000.0) +. Float.of_int status.ms_before_next)
    /. 1000.0
  in
  let headers =
    [ ("RateLimit-Limit", Int.to_string status.limit)
    ; ("RateLimit-Reset", Int.to_string (Float.to_int reset_at))
    ; ("RateLimit-Remaining", Int.to_string status.remaining_points)
    ; ( "RateLimit-Policy"
      , Printf.sprintf "%d;w=%d" status.limit (status.duration_ms / 1000) ) ]
  in
  Dream.json ~status:`Too_Many_Requests ~headers
    {|{"error":"RateLimitExceeded","message":"Rate limit exceeded"}|}

let extract_nsid req = (Dream.path [@warning "-3"]) req |> List.rev |> List.hd

let handler ?(auth : Auth.Verifiers.t = Any)
    ?(rate_limits : rate_limit_rule list = []) (hdlr : handler) (init : init) =
  let open Errors in
  try
    let auth = Auth.Verifiers.of_t auth in
    try%lwt
      match%lwt auth init with
      | Ok creds -> (
          let ctx = {req= init.req; db= init.db; auth= creds} in
          try
            let nsid = extract_nsid init.req in
            apply_rate_limits ~nsid rate_limits ctx ;
            try%lwt hdlr ctx
            with e ->
              if not (is_xrpc_error e) then log_exn e ;
              exn_to_response e
          with Rate_limiter.Rate_limit_exceeded status ->
            rate_limit_response status )
      | Error e ->
          exn_to_response e
    with
    | Redirect r ->
        Dream.redirect init.req r
    | Rate_limiter.Rate_limit_exceeded status ->
        rate_limit_response status
    | e ->
        if not (is_xrpc_error e) then log_exn e ;
        exn_to_response e
  with Redirect r -> Dream.redirect init.req r

let parse_query (req : Dream.request)
    (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a =
  try
    let queries = Dream.all_queries req in
    let query_json =
      `Assoc
        (List.map
           (fun (k, v) ->
             (k, try Yojson.Safe.from_string v with _ -> `String v) )
           queries )
    in
    match query_json |> of_yojson with
    | Error e ->
        Log.debug (fun log -> log "error parsing query: %s" e) ;
        Errors.internal_error ()
    | Ok query ->
        query
  with _ -> Errors.invalid_request "invalid query string"

let parse_body (req : Dream.request)
    (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a Lwt.t =
  try%lwt
    let%lwt body_assoc =
      match Dream.header req "content-type" with
      | None ->
          Lwt.return (`Assoc [])
      | Some content_type -> (
        match String.split_on_char ';' content_type with
        | "application/x-www-form-urlencoded" :: _ -> (
          match%lwt Dream.form ~csrf:false req with
          | `Ok form ->
              Lwt.return
                (`Assoc
                   (List.map
                      (fun (k, v) ->
                        (k, try Yojson.Safe.from_string v with _ -> `String v) )
                      form ) )
          | _ ->
              Errors.internal_error () )
        | "application/json" :: _ ->
            let%lwt body = Dream.body req in
            Lwt.return @@ Yojson.Safe.from_string body
        | _ ->
            Lwt.return (`Assoc []) )
    in
    Log.debug (fun l -> l "body: %s" (Yojson.Safe.to_string body_assoc)) ;
    match of_yojson body_assoc with
    | Error e ->
        Log.debug (fun log -> log "error parsing body: %s" e) ;
        Errors.internal_error ()
    | Ok body ->
        Lwt.return body
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
  let fragment = "#" ^ service_type in
  let aud = Option.value aud ~default:service_did in
  let lxm = Option.value lxm ~default:nsid in
  let rpc_aud = aud ^ fragment in
  Auth.assert_rpc_scope ctx.auth ~aud:rpc_aud ~lxm ;
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
      let path, _ = Dream.split_target (Dream.target ctx.req) in
      let query = Util.copy_query ctx.req in
      let uri = Uri.make ~scheme ~host ~path ~query () in
      let headers =
        Util.make_headers
          [ ("accept-language", Dream.header ctx.req "accept-language")
          ; ("content-type", Dream.header ctx.req "content-type")
          ; ( "atproto-accept-labelers"
            , Dream.header ctx.req "atproto-accept-labelers" )
          ; ("authorization", Some ("Bearer " ^ jwt)) ]
      in
      match Dream.method_ ctx.req with
      | `GET -> (
          let%lwt res, body =
            try%lwt
              Lwt_unix.with_timeout 15.0 (fun () -> Util.http_get uri ~headers)
            with Lwt_unix.Timeout ->
              Errors.internal_error ~msg:"proxy request timed out" ()
          in
          let res_headers =
            Cohttp.Response.headers res |> Cohttp.Header.to_list
          in
          match res.status with
          | `OK ->
              Dream.stream ~status:`OK ~headers:res_headers (fun stream ->
                  Body.to_stream body |> Lwt_stream.iter_s (Dream.write stream) )
          | e ->
              let%lwt () = Body.drain_body body in
              Log.err (fun log ->
                  log "error when proxying to %s: %s" (Uri.to_string uri)
                    (Http.Status.to_string e) ) ;
              Errors.internal_error ~msg:"failed to proxy request" () )
      | `POST -> (
          let%lwt req_body = Dream.body ctx.req in
          let%lwt res, body =
            try%lwt
              Lwt_unix.with_timeout 15.0 (fun () ->
                  Client.post uri ~headers ~body:(Body.of_string req_body) )
            with Lwt_unix.Timeout ->
              Errors.internal_error ~msg:"proxy request timed out" ()
          in
          let res_headers =
            Cohttp.Response.headers res |> Cohttp.Header.to_list
          in
          match res.status with
          | `OK ->
              Dream.stream ~status:`OK ~headers:res_headers (fun stream ->
                  Body.to_stream body |> Lwt_stream.iter_s (Dream.write stream) )
          | e ->
              let%lwt () = Body.drain_body body in
              Log.err (fun log ->
                  log "error when proxying to %s: %s" (Uri.to_string uri)
                    (Http.Status.to_string e) ) ;
              Errors.internal_error ~msg:"failed to proxy request" () )
      | _ ->
          Errors.invalid_request "unsupported method" )
  | Error e ->
      Log.err (fun log -> log "error when resolving destination service: %s" e) ;
      Errors.internal_error ~msg:"failed to resolve destination service" ()

let service_proxy_handler db req =
  match Dream.header req "atproto-proxy" with
  | Some _ ->
      handler ~auth:Authorization service_proxy {req; db}
  | None ->
      Dream.empty `Not_Found

let dpop_middleware inner_handler req =
  let%lwt res = inner_handler req in
  let dpop, www_auth =
    (Dream.header req "DPoP", Dream.header res "WWW-Authenticate")
  in
  if
    Option.is_some dpop
    || Option.is_some www_auth
       && Option.get www_auth |> Util.str_contains ~affix:"DPoP"
  then begin
    Dream.set_header res "DPoP-Nonce" (Oauth.Dpop.next_nonce ()) ;
    Dream.add_header res "Access-Control-Expose-Headers"
      "DPoP-Nonce, WWW-Authenticate"
  end ;
  Lwt.return res

let cors_middleware inner_handler req =
  let%lwt res = inner_handler req in
  let origin = Dream.header req "Origin" in
  Dream.set_header res "Access-Control-Allow-Origin"
    (Option.value origin ~default:"*") ;
  Dream.set_header res "Access-Control-Allow-Methods"
    "GET, POST, PUT, DELETE, OPTIONS" ;
  Dream.set_header res "Access-Control-Allow-Headers" "*" ;
  Dream.set_header res "Access-Control-Max-Age" "86400" ;
  Lwt.return res

let resolve_repo_did ctx repo =
  match%lwt Data_store.get_actor_by_identifier repo ctx.db with
  | Some {did; _} ->
      Lwt.return did
  | None ->
      Errors.invalid_request "target repository not found"

let resolve_repo_did_authed ctx repo =
  let%lwt input_did = resolve_repo_did ctx repo in
  let did =
    match ctx.auth with
    | (Access {did} | OAuth {did; _}) when did = input_did ->
        did
    | Admin ->
        input_did
    | _ ->
        Errors.auth_required "authentication does not match target repository"
  in
  Lwt.return did
