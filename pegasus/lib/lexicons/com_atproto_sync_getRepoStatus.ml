(* generated from com.atproto.sync.getRepoStatus *)

(** Get the hosting status for a repository, on this server. Expected to be implemented by PDS and Relay. *)
module Main = struct
  let nsid = "com.atproto.sync.getRepoStatus"

  type params =
  {
    did: string;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    did: string;
    active: bool;
    status: string option [@default None];
    rev: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ~did
      (client : Hermes.client) : output Lwt.t =
    let params : params = {did} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

