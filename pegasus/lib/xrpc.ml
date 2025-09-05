open Util.Exceptions

type init = Auth.Verifiers.ctx

type context = {req: Dream.request; db: Data_store.t; auth: Auth.credentials}

type handler = context -> Dream.response Lwt.t

let handler ?(auth : Auth.Verifiers.verifier = Auth.Verifiers.unauthenticated)
    (hdlr : handler) (init : init) =
  match%lwt auth init with
  | Ok creds -> (
      try%lwt hdlr {req= init.req; db= init.db; auth= creds}
      with e -> log_exn e ; exn_to_response e )
  | Error e ->
      exn_to_response e

let parse_body (req : Dream.request)
    (of_yojson : Yojson.Safe.t -> ('a, string) result) : 'a Lwt.t =
  try%lwt
    let%lwt body = Dream.body req in
    body |> Yojson.Safe.from_string |> of_yojson |> Result.get_ok |> Lwt.return
  with _ -> Errors.invalid_request "Invalid request body"
