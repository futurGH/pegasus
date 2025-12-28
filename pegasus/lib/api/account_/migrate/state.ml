(* migration state management *)

type t =
  { did: string
  ; handle: string
  ; old_pds: string
  ; access_jwt: string
  ; refresh_jwt: string
  ; email: string
  ; blobs_imported: int
  ; blobs_failed: int
  ; blobs_cursor: string
  ; plc_requested: bool }
[@@deriving yojson]

let state_key = "pegasus.migration_state"

let get req =
  match Dream.session_field req state_key with
  | Some json -> (
    match of_yojson (Yojson.Safe.from_string json) with
    | Ok state ->
        Some state
    | Error _ ->
        None )
  | None ->
      None

let set req state =
  Dream.set_session_field req state_key
    (Yojson.Safe.to_string (to_yojson state))

let clear req = Dream.drop_session_field req state_key

(* possible states for resuming an existing deactivated account *)
type resume_state =
  | NeedsRepoImport
  | NeedsBlobImport
  | NeedsPlcUpdate
  | NeedsActivation
  | AlreadyActive

let normalize_endpoint s =
  if String.length s > 0 && s.[String.length s - 1] = '/' then
    String.sub s 0 (String.length s - 1)
  else s

let check_identity_updated did =
  match%lwt Id_resolver.Did.resolve ~skip_cache:true did with
  | Error e ->
      Lwt.return_error ("Failed to resolve DID: " ^ e)
  | Ok doc -> (
    match Id_resolver.Did.Document.get_service doc "#atproto_pds" with
    | None ->
        Lwt.return_error "DID document missing PDS service"
    | Some endpoint ->
        let normalized_endpoint = normalize_endpoint endpoint in
        let normalized_host = normalize_endpoint Env.host_endpoint in
        if normalized_endpoint = normalized_host then Lwt.return_ok true
        else Lwt.return_ok false )

let check_resume_state ~did db =
  match%lwt Data_store.get_actor_by_identifier did db with
  | None ->
      Lwt.return_error "Account not found"
  | Some actor when actor.deactivated_at = None ->
      Lwt.return_ok AlreadyActive
  | Some _actor -> (
    match%lwt check_identity_updated did with
    | Ok true ->
        Lwt.return_ok NeedsActivation
    | _ -> (
      try%lwt
        let%lwt us = User_store.connect ~create:false did in
        let%lwt record_count = User_store.count_records us in
        if record_count > 0 then
          match%lwt User_store.count_blobs us with
          | cnt when cnt > 0 ->
              Lwt.return_ok NeedsPlcUpdate
          | _ ->
              Lwt.return_ok NeedsBlobImport
        else Lwt.return_ok NeedsRepoImport
      with _ -> Lwt.return_ok NeedsRepoImport ) )
