(* generated from com.atproto.moderation.createReport *)

type mod_tool = {name: string; meta: Yojson.Safe.t option [@default None]}
[@@deriving yojson {strict= false}]

(** Submit a moderation report regarding an atproto account or record. Implemented by moderation services (with PDS proxying), and requires auth. *)
module Main = struct
  let nsid = "com.atproto.moderation.createReport"

  type params = unit

  let params_to_yojson () = `Assoc []

  type subject =
    | RepoRef of Com_atproto_admin_defs.repo_ref
    | StrongRef of Com_atproto_repo_strongRef.main
    | Unknown of Yojson.Safe.t

  let subject_of_yojson json =
    let open Yojson.Safe.Util in
    try
      match json |> member "$type" |> to_string with
      | "com.atproto.admin.defs#repoRef" -> (
        match Com_atproto_admin_defs.repo_ref_of_yojson json with
        | Ok v ->
            Ok (RepoRef v)
        | Error e ->
            Error e )
      | "com.atproto.repo.strongRef" -> (
        match Com_atproto_repo_strongRef.main_of_yojson json with
        | Ok v ->
            Ok (StrongRef v)
        | Error e ->
            Error e )
      | _ ->
          Ok (Unknown json)
    with _ -> Error "failed to parse union"

  let subject_to_yojson = function
    | RepoRef v -> (
      match Com_atproto_admin_defs.repo_ref_to_yojson v with
      | `Assoc fields ->
          `Assoc (("$type", `String "com.atproto.admin.defs#repoRef") :: fields)
      | other ->
          other )
    | StrongRef v -> (
      match Com_atproto_repo_strongRef.main_to_yojson v with
      | `Assoc fields ->
          `Assoc (("$type", `String "com.atproto.repo.strongRef") :: fields)
      | other ->
          other )
    | Unknown j ->
        j

  type input =
    { reason_type: Com_atproto_moderation_defs.reason_type [@key "reasonType"]
    ; reason: string option [@default None]
    ; subject: subject
    ; mod_tool: mod_tool option [@key "modTool"] [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { id: int
    ; reason_type: Com_atproto_moderation_defs.reason_type [@key "reasonType"]
    ; reason: string option [@default None]
    ; subject: subject
    ; reported_by: string [@key "reportedBy"]
    ; created_at: string [@key "createdAt"] }
  [@@deriving yojson {strict= false}]

  let call ~reason_type ?reason ~subject ?mod_tool (client : Hermes.client) :
      output Lwt.t =
    let params = () in
    let input =
      Some ({reason_type; reason; subject; mod_tool} |> input_to_yojson)
    in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
