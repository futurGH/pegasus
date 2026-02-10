(* generated from app.bsky.notification.listActivitySubscriptions *)

(** Enumerate all accounts to which the requesting account is subscribed to receive notifications for. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.listActivitySubscriptions"

  type params =
    {limit: int option [@default None]; cursor: string option [@default None]}
  [@@deriving yojson {strict= false}]

  type output =
    { cursor: string option [@default None]
    ; subscriptions: App_bsky_actor_defs.profile_view list }
  [@@deriving yojson {strict= false}]

  let call ?limit ?cursor (client : Hermes.client) : output Lwt.t =
    let params : params = {limit; cursor} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
