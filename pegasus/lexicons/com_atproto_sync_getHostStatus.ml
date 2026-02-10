(* generated from com.atproto.sync.getHostStatus *)

(** Returns information about a specified upstream host, as consumed by the server. Implemented by relays. *)
module Main = struct
  let nsid = "com.atproto.sync.getHostStatus"

  type params = {hostname: string} [@@deriving yojson {strict= false}]

  type output =
    { hostname: string
    ; seq: int option [@default None]
    ; account_count: int option [@key "accountCount"] [@default None]
    ; status: Com_atproto_sync_defs.host_status option [@default None] }
  [@@deriving yojson {strict= false}]

  let call ~hostname (client : Hermes.client) : output Lwt.t =
    let params : params = {hostname} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
