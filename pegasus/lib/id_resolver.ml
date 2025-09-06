open Cohttp_lwt
open Cohttp_lwt_unix

let did_regex =
  Str.regexp {|^did:([a-z]+):([a-zA-Z0-9._:%\-]*[a-zA-Z0-9._\-])$|}

module Handle = struct
  let dns_client = Dns_client_unix.create ()

  let resolve_well_known handle =
    try%lwt
      let uri =
        Uri.of_string ("https://" ^ handle ^ "/.well-known/atproto-did")
      in
      let%lwt _, body = Client.get uri in
      let%lwt did = Body.to_string body in
      Lwt.return_ok did
    with exn -> Lwt.return_error (Printexc.to_string exn)

  let resolve_dns handle =
    try%lwt
      match
        Dns_client_unix.getaddrinfo dns_client Dns.Rr_map.Txt
          (Domain_name.of_string_exn handle)
      with
      | Ok (_, t) -> (
          let txt = Dns.Rr_map.Txt_set.choose t in
          match Str.string_match did_regex txt 0 with
          | true -> (
              let method_name = Str.matched_group 1 txt in
              let id = Str.matched_group 2 txt in
              match method_name with
              | "web" | "plc" ->
                  Lwt.return_ok ("did:" ^ method_name ^ ":" ^ id)
              | _ ->
                  Lwt.return_error "unsupported method" )
          | false ->
              Lwt.return_error "invalid txt record" )
      | Error (`Msg e) ->
          Lwt.return_error e
    with exn -> Lwt.return_error (Printexc.to_string exn)

  let resolve handle =
    (* run well-known and dns in parallel, error if they return different values, if only one returns just return that value *)
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
             "well-known resolution error: %s\ndns resolution error: %s" e1 e2 )
    | _ ->
        Lwt.return_error "unexpected error"
end
