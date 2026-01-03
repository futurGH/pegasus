(* generated from com.atproto.sync.getRepo *)

(** Download a repository export as CAR file. Optionally only a 'diff' since a previous revision. Does not require auth; implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.sync.getRepo"

  type params =
  {
    did: string;
    since: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  (** raw bytes output with content type *)
  type output = string * string

  let call
      ~did
      ?since
      (client : Hermes.client) : output Lwt.t =
    let params : params = {did; since} in
    Hermes.query_bytes client nsid (params_to_yojson params)
end

