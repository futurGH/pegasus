open Cohttp_lwt

module Handle = struct
  let dns_client = Dns_client_lwt.create (Happy_eyeballs_lwt.create ())

  let resolve_well_known handle =
    try%lwt
      let uri =
        Uri.of_string ("https://" ^ handle ^ "/.well-known/atproto-did")
      in
      let%lwt {status; _}, body = Util.http_get uri in
      match status with
      | `OK ->
          let%lwt did = Body.to_string body in
          if
            String.starts_with ~prefix:"did:plc:" did
            || String.starts_with ~prefix:"did:web:" did
          then Lwt.return_ok (String.trim did)
          else Lwt.return_error "invalid did in .well-known/atproto-did"
      | _ ->
          let%lwt () = Body.drain_body body in
          Lwt.return_error "failed to resolve"
    with exn -> Lwt.return_error (Printexc.to_string exn)

  let resolve_dns handle =
    try%lwt
      let%lwt result =
        Dns_client_lwt.getaddrinfo dns_client Dns.Rr_map.Txt
          (Domain_name.of_string_exn ("_atproto." ^ handle))
      in
      match result with
      | Ok (_, t) -> (
          let txt = Dns.Rr_map.Txt_set.choose t in
          match String.split_on_char '=' txt with
          | ["did"; did]
            when String.starts_with ~prefix:"did:plc:" did
                 || String.starts_with ~prefix:"did:web:" did ->
              Lwt.return_ok (String.trim did)
          | _ ->
              Lwt.return_error "invalid did in dns record" )
      | Error (`Msg e) ->
          Lwt.return_error e
    with exn -> Lwt.return_error (Printexc.to_string exn)

  let cache = Ttl_cache.String_cache.create (3 * 60 * 60 * 1000) ()

  let resolve ?(skip_cache = false) handle =
    match Ttl_cache.String_cache.get cache handle with
    | Some from_cache when skip_cache = false ->
        Lwt.return_ok from_cache
    | _ -> (
        (* run well-known and dns in parallel, error if they return different values, if only one returns just return that value *)
        let%lwt result =
          match%lwt Lwt.all [resolve_well_known handle; resolve_dns handle] with
          | [Ok did1; Ok did2] when did1 = did2 ->
              Lwt.return_ok did1
          | [Ok _; Ok _] ->
              Lwt.return_error "conflicting dids"
          | [Ok did1; _] ->
              Lwt.return_ok did1
          | [_; Ok did2] ->
              Lwt.return_ok did2
          | [Error e1; Error e2] ->
              Lwt.return_error
                (Printf.sprintf
                   "well-known resolution error: %s\ndns resolution error: %s"
                   e1 e2 )
          | _ ->
              Lwt.return_error "unexpected error"
        in
        match result with
        | Ok did ->
            Ttl_cache.String_cache.set cache handle did ;
            Lwt.return_ok did
        | Error e ->
            Lwt.return_error e )
end

module Did = struct
  open Util.Did_doc_types

  module Document = struct
    type service =
      { id: string
      ; type': string_or_strings [@key "type"]
      ; service_endpoint: string_or_string_map_or_either_list
            [@key "serviceEndpoint"] }
    [@@deriving yojson {strict= false}]

    type verification_method =
      { id: string
      ; type': string [@key "type"]
      ; controller: string
      ; public_key_multibase: string option
            [@key "publicKeyMultibase"] [@default None] }
    [@@deriving yojson {strict= false}]

    type string_or_verification_method =
      [`String of string | `VerificationMethod of verification_method]

    let string_or_verification_method_to_yojson = function
      | `String s ->
          `String s
      | `VerificationMethod vm ->
          verification_method_to_yojson vm

    let string_or_verification_method_of_yojson = function
      | `String s ->
          Ok (`String s)
      | `Assoc m ->
          verification_method_of_yojson (`Assoc m)
          |> Result.map (fun x -> `VerificationMethod x)
      | _ ->
          Error "invalid field value"

    type t =
      { context: string list option [@key "@context"] [@default None]
      ; id: string
      ; controller: string_or_strings option [@default None]
      ; also_known_as: string list option [@key "alsoKnownAs"] [@default None]
      ; verification_method: verification_method list option
            [@key "verificationMethod"] [@default None]
      ; authentication: string_or_verification_method list option
            [@default None]
      ; service: service list option [@default None] }
    [@@deriving yojson {strict= false}]

    let get_service_endpoint s =
      match s.service_endpoint with
      | `String e ->
          e
      | `List l -> (
        match List.hd l with
        | `String e ->
            e
        | `String_map m ->
            List.hd m |> snd )
      | `String_map m ->
          List.hd m |> snd

    let get_service t fragment =
      match t.service with
      | None ->
          None
      | Some services ->
          List.find_map
            (fun (s : service) ->
              if s.id = fragment then Some (get_service_endpoint s) else None )
            services

    let get_verification_key t fragment =
      match t.verification_method with
      | None ->
          None
      | Some methods ->
          List.find_map
            (fun (vm : verification_method) ->
              if vm.id = fragment || vm.id = t.id ^ fragment then
                vm.public_key_multibase
              else None )
            methods
  end

  type document = Document.t

  let cache = Ttl_cache.String_cache.create (12 * 60 * 60 * 1000) ()

  let resolve_plc did =
    if not (String.starts_with ~prefix:"did:plc:" did) then
      Lwt.return_error "invalid did method"
    else
      try%lwt
        let uri =
          Uri.make ~scheme:"https" ~host:"plc.directory"
            ~path:(Uri.pct_encode did) ()
        in
        let%lwt {status; _}, body =
          Util.http_get uri
            ~headers:(Cohttp.Header.of_list [("Accept", "application/json")])
        in
        match status with
        | `OK ->
            let%lwt body = Body.to_string body in
            body |> Yojson.Safe.from_string |> Document.of_yojson |> Lwt.return
        | _ ->
            let%lwt () = Body.drain_body body in
            Lwt.return_error "failed to resolve"
      with e -> Lwt.return_error (Printexc.to_string e)

  let resolve_web did =
    if not (String.starts_with ~prefix:"did:web:" did) then
      Lwt.return_error "invalid did method"
    else
      try%lwt
        let uri =
          Uri.make ~scheme:"https" ~host:(Str.string_after did 8)
            ~path:"/.well-known/did.json" ()
        in
        let%lwt {status; _}, body =
          Util.http_get uri
            ~headers:(Cohttp.Header.of_list [("Accept", "application/json")])
        in
        match status with
        | `OK ->
            let%lwt body = Body.to_string body in
            body |> Yojson.Safe.from_string |> Document.of_yojson |> Lwt.return
        | _ ->
            let%lwt () = Body.drain_body body in
            Lwt.return_error "failed to resolve"
      with e -> Lwt.return_error (Printexc.to_string e)

  let resolve ?(skip_cache = false) did =
    match Ttl_cache.String_cache.get cache did with
    | Some from_cache when skip_cache = false ->
        Lwt.return_ok from_cache
    | _ -> (
        let%lwt result =
          match did with
          | did when String.starts_with ~prefix:"did:plc:" did ->
              resolve_plc did
          | did when String.starts_with ~prefix:"did:web:" did ->
              resolve_web did
          | _ ->
              Lwt.return_error "invalid did method"
        in
        match result with
        | Ok doc ->
            Ttl_cache.String_cache.set cache did doc ;
            Lwt.return_ok doc
        | Error err ->
            Lwt.return_error err )
end
