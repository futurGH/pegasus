(* generated from com.atproto.repo.getRecord *)

(** Get a single record from a repository. Does not require auth. *)
module Main = struct
  let nsid = "com.atproto.repo.getRecord"

  type params =
    { repo: string
    ; collection: string
    ; rkey: string
    ; cid: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    {uri: string; cid: string option [@default None]; value: Yojson.Safe.t}
  [@@deriving yojson {strict= false}]

  let call ~repo ~collection ~rkey ?cid (client : Hermes.client) : output Lwt.t
      =
    let params : params = {repo; collection; rkey; cid} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
