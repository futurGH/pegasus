(* generated from com.atproto.sync.listReposByCollection *)

type repo =
  {
    did: string;
  }
[@@deriving yojson {strict= false}]

(** Enumerates all the DIDs which have records with the given collection NSID. *)
module Main = struct
  let nsid = "com.atproto.sync.listReposByCollection"

  type params =
  {
    collection: string;
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
      ~collection
      ?limit
      ?cursor
      (client : Hermes.client) : output Lwt.t =
    let params : params = {collection; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

