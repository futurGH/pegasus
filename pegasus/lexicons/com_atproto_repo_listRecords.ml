(* generated from com.atproto.repo.listRecords *)

type record = {uri: string; cid: string; value: Yojson.Safe.t}
[@@deriving yojson {strict= false}]

(** List a range of records in a repository, matching a specific collection. Does not require auth. *)
module Main = struct
  let nsid = "com.atproto.repo.listRecords"

  type params =
    { repo: string
    ; collection: string
    ; limit: int option [@default None]
    ; cursor: string option [@default None]
    ; reverse: bool option [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {cursor: string option [@default None]; records: record list}
  [@@deriving yojson {strict= false}]

  let call ~repo ~collection ?limit ?cursor ?reverse (client : Hermes.client) :
      output Lwt.t =
    let params : params = {repo; collection; limit; cursor; reverse} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
