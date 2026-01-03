(* generated from app.bsky.notification.listNotifications *)

type notification =
  {
    uri: string;
    cid: string;
    author: App_bsky_actor_defs.profile_view;
    reason: string;
    reason_subject: string option [@key "reasonSubject"] [@default None];
    record: Yojson.Safe.t;
    is_read: bool [@key "isRead"];
    indexed_at: string [@key "indexedAt"];
    labels: Com_atproto_label_defs.label list option [@default None];
  }
[@@deriving yojson {strict= false}]

(** Enumerate notifications for the requesting account. Requires auth. *)
module Main = struct
  let nsid = "app.bsky.notification.listNotifications"

  type params =
  {
    reasons: string list option [@default None] [@of_yojson Hermes_util.query_string_list_option_of_yojson] [@to_yojson Hermes_util.query_string_list_option_to_yojson];
    limit: int option [@default None];
    priority: bool option [@default None];
    cursor: string option [@default None];
    seen_at: string option [@key "seenAt"] [@default None];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cursor: string option [@default None];
    notifications: notification list;
    priority: bool option [@default None];
    seen_at: string option [@key "seenAt"] [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ?reasons
      ?limit
      ?priority
      ?cursor
      ?seen_at
      (client : Hermes.client) : output Lwt.t =
    let params : params = {reasons; limit; priority; cursor; seen_at} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

