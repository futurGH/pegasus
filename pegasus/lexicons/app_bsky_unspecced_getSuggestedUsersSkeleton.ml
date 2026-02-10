(* generated from app.bsky.unspecced.getSuggestedUsersSkeleton *)

(** Get a skeleton of suggested users. Intended to be called and hydrated by app.bsky.unspecced.getSuggestedUsers *)
module Main = struct
  let nsid = "app.bsky.unspecced.getSuggestedUsersSkeleton"

  type params =
    { viewer: string option [@default None]
    ; category: string option [@default None]
    ; limit: int option [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {dids: string list} [@@deriving yojson {strict= false}]

  let call ?viewer ?category ?limit (client : Hermes.client) : output Lwt.t =
    let params : params = {viewer; category; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
