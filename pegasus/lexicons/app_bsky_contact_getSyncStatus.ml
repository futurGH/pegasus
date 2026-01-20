(* generated from app.bsky.contact.getSyncStatus *)

(** Gets the user's current contact import status. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.contact.getSyncStatus"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output =
    { sync_status: App_bsky_contact_defs.sync_status option
          [@key "syncStatus"] [@default None] }
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
