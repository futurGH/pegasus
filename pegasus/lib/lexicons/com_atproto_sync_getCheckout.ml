(* generated from com.atproto.sync.getCheckout *)

(** DEPRECATED - please use com.atproto.sync.getRepo instead *)
module Main = struct
  let nsid = "com.atproto.sync.getCheckout"

  type params =
  {
    did: string;
  }
[@@deriving yojson {strict= false}]

  (** raw bytes output with content type *)
  type output = string * string

  let call
      ~did
      (client : Hermes.client) : output Lwt.t =
    let params : params = {did} in
    Hermes.query_bytes client nsid (params_to_yojson params)
end

