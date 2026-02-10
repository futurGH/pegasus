open Lexicons.Com.Atproto.Admin.GetAccountInfo.Main

let actor_to_account_view (actor : Data_store.Types.actor) : output =
  { did= actor.did
  ; handle= actor.handle
  ; email= Some actor.email
  ; email_confirmed_at=
      Option.map Util.Time.ms_to_iso8601 actor.email_confirmed_at
  ; indexed_at= Util.Time.ms_to_iso8601 actor.created_at
  ; deactivated_at= Option.map Util.Time.ms_to_iso8601 actor.deactivated_at
  ; related_records= None
  ; invited_by= None
  ; invites= None
  ; invites_disabled= None
  ; invite_note= None
  ; threat_signatures= None }

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let did = Dream.query req "did" |> Option.value ~default:"" in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.invalid_request "account not found"
      | Some actor ->
          let response = actor_to_account_view actor in
          Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson response )
