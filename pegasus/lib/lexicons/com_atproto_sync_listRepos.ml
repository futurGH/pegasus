(* generated from com.atproto.sync.listRepos *)

type repo =
  {
    did: string;
    head: string;
    rev: string;
    active: bool option [@default None];
    status: string option [@default None];
  }
[@@deriving yojson {strict= false}]

(** Enumerates all the DID, rev, and commit CID for all repos hosted by this service. Does not require auth; implemented by PDS and Relay. *)
module Main = struct
  let nsid = "com.atproto.sync.listRepos"

  type params =
  {
    limit: int option [@default None];
    cursor: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    repos: repo list;
  }
[@@deriving yojson {strict= false}]

  let call
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

