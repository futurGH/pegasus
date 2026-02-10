(* generated from app.bsky.notification.getUnreadCount *)

(** Count the number of unread notifications for the requesting account. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.getUnreadCount"

  type params =
    { priority: bool option [@default None]
    ; seen_at: string option [@key "seenAt"] [@default None] }
  [@@deriving yojson {strict= false}]

  type output = {count: int} [@@deriving yojson {strict= false}]

  let call ?priority ?seen_at (client : Hermes.client) : output Lwt.t =
    let params : params = {priority; seen_at} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
