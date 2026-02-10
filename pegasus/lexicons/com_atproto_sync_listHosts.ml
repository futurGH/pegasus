(* generated from com.atproto.sync.listHosts *)

type host =
  { hostname: string
  ; seq: int option [@default None]
  ; account_count: int option [@key "accountCount"] [@default None]
  ; status: Com_atproto_sync_defs.host_status option [@default None] }
[@@deriving yojson {strict= false}]

(** Enumerates upstream hosts (eg, PDS or relay instances) that this service consumes from. Implemented by relays. *)
module Main = struct
  let nsid = "com.atproto.sync.listHosts"

  type params =
    {limit: int option [@default None]; cursor: string option [@default None]}
  [@@deriving yojson {strict= false}]

  type output = {cursor: string option [@default None]; hosts: host list}
  [@@deriving yojson {strict= false}]

  let call ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
