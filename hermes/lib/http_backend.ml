(* abstract http backend for dependency injection *)

type response = Cohttp.Response.t * Cohttp_lwt.Body.t

module type S = sig
  val get : headers:Cohttp.Header.t -> Uri.t -> response Lwt.t

  val post :
    headers:Cohttp.Header.t -> body:Cohttp_lwt.Body.t -> Uri.t -> response Lwt.t
end

(* default implementation using cohttp-lwt-unix *)
module Default : S = struct
  let get ~headers uri = Cohttp_lwt_unix.Client.get ~headers uri

  let post ~headers ~body uri = Cohttp_lwt_unix.Client.post ~headers ~body uri
end
