(* generated from com.atproto.sync.getRecord *)

(** Get data blocks needed to prove the existence or non-existence of record in the current version of repo. Does not require auth. *)
module Main = struct
  let nsid = "com.atproto.sync.getRecord"

  type params =
  {
    did: string;
    collection: string;
    rkey: string;
  }
[@@deriving yojson {strict= false}]

  (** raw bytes output with content type *)
  type output = string * string

  let call
      ~did
      ~collection
      ~rkey
      (client : Hermes.client) : output Lwt.t =
    let params : params = {did; collection; rkey} in
    Hermes.query_bytes client nsid (params_to_yojson params)
end

