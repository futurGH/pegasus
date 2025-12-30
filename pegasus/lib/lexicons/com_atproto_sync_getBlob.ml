(* generated from com.atproto.sync.getBlob *)

(** Get a blob associated with a given account. Returns the full blob as originally uploaded. Does not require auth; implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.sync.getBlob"

  type params =
  {
    did: string;
    cid: string;
  }
[@@deriving yojson {strict= false}]

  (** Raw bytes output with content type *)
  type output = string * string

  let call
      ~did
      ~cid
      (client : Hermes.client) : output Lwt.t =
    let params : params = {did; cid} in
    Hermes.query_bytes client nsid (params_to_yojson params)
end

