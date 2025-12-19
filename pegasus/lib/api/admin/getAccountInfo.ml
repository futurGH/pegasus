type account_view =
  { did: string
  ; handle: string
  ; email: string
  ; email_confirmed_at: string option [@key "emailConfirmedAt"]
  ; indexed_at: string [@key "indexedAt"]
  ; deactivated_at: string option [@key "deactivatedAt"] }
[@@deriving yojson {strict= false}]

type response = account_view [@@deriving yojson {strict= false}]

let actor_to_account_view (actor : Data_store.Types.actor) : account_view =
  { did= actor.did
  ; handle= actor.handle
  ; email= actor.email
  ; email_confirmed_at= Option.map Util.ms_to_iso8601 actor.email_confirmed_at
  ; indexed_at= Util.ms_to_iso8601 actor.created_at
  ; deactivated_at= Option.map Util.ms_to_iso8601 actor.deactivated_at }

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let did = Dream.query req "did" |> Option.value ~default:"" in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.invalid_request "account not found"
      | Some actor ->
          let response = actor_to_account_view actor in
          Dream.json @@ Yojson.Safe.to_string @@ response_to_yojson response )
