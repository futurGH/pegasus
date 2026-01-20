(* generated from app.bsky.contact.defs *)

type match_and_contact_index =
  { match_: App_bsky_actor_defs.profile_view [@key "match"]
  ; contact_index: int [@key "contactIndex"] }
[@@deriving yojson {strict= false}]

type sync_status =
  {synced_at: string [@key "syncedAt"]; matches_count: int [@key "matchesCount"]}
[@@deriving yojson {strict= false}]

type notification = {from: string; to_: string [@key "to"]}
[@@deriving yojson {strict= false}]
