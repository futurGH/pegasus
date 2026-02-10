(* generated from com.atproto.sync.listBlobs *)

(** List blob CIDs for an account, since some repo revision. Does not require auth; implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.sync.listBlobs"

  type params =
    { did: string
    ; since: string option [@default None]
    ; limit: int option [@default None]
    ; cursor: string option [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {cursor: string option [@default None]; cids: string list}
  [@@deriving yojson {strict= false}]

  let call ~did ?since ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {did; since; limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
