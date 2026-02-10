(* generated from com.atproto.admin.updateSubjectStatus *)

(** Update the service-specific admin status of a subject (account, record, or blob). *)
module Main = struct
  let nsid = "com.atproto.admin.updateSubjectStatus"

  type params = unit

  let params_to_yojson () = `Assoc []

  type subject =
    | RepoRef of Com_atproto_admin_defs.repo_ref
    | StrongRef of Com_atproto_repo_strongRef.main
    | RepoBlobRef of Com_atproto_admin_defs.repo_blob_ref
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
      | "com.atproto.admin.defs#repoBlobRef" -> (
        match Com_atproto_admin_defs.repo_blob_ref_of_yojson json with
        | Ok v ->
            Ok (RepoBlobRef v)
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
    | RepoBlobRef v -> (
      match Com_atproto_admin_defs.repo_blob_ref_to_yojson v with
      | `Assoc fields ->
          `Assoc
            (("$type", `String "com.atproto.admin.defs#repoBlobRef") :: fields)
      | other ->
          other )
    | Unknown j ->
        j

  type input =
    { subject: subject
    ; takedown: Com_atproto_admin_defs.status_attr option [@default None]
    ; deactivated: Com_atproto_admin_defs.status_attr option [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    { subject: subject
    ; takedown: Com_atproto_admin_defs.status_attr option [@default None] }
  [@@deriving yojson {strict= false}]

  let call ~subject ?takedown ?deactivated (client : Hermes.client) :
      output Lwt.t =
    let params = () in
    let input = Some ({subject; takedown; deactivated} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
