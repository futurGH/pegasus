open Util.Exceptions

type init = Auth.Verifiers.ctx

type context = {req: Dream.request; db: Data_store.t; auth: Auth.credentials}

type handler = context -> Dream.response Lwt.t

let handler ?(auth : Auth.Verifiers.verifier = Auth.Verifiers.unauthenticated)
    (hdlr : handler) (init : init) =
  match%lwt auth init with
  | Ok creds -> (
      try%lwt hdlr {req= init.req; db= init.db; auth= creds}
      with e -> exn_to_response e )
  | Error e ->
      exn_to_response e
