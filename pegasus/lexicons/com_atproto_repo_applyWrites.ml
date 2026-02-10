(* generated from com.atproto.repo.applyWrites *)

type delete_result = unit

let delete_result_of_yojson _ = Ok ()

let delete_result_to_yojson () = `Assoc []

type update_result =
  { uri: string
  ; cid: string
  ; validation_status: string option [@key "validationStatus"] [@default None]
  }
[@@deriving yojson {strict= false}]

type create_result =
  { uri: string
  ; cid: string
  ; validation_status: string option [@key "validationStatus"] [@default None]
  }
[@@deriving yojson {strict= false}]

type delete = {collection: string; rkey: string}
[@@deriving yojson {strict= false}]

type update = {collection: string; rkey: string; value: Yojson.Safe.t}
[@@deriving yojson {strict= false}]

type create =
  {collection: string; rkey: string option [@default None]; value: Yojson.Safe.t}
[@@deriving yojson {strict= false}]

(** Apply a batch transaction of repository creates, updates, and deletes. Requires auth, implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.repo.applyWrites"

  type params = unit

  let params_to_yojson () = `Assoc []

  type writes_item = Create of create | Update of update | Delete of delete

  let writes_item_of_yojson json =
    let open Yojson.Safe.Util in
    try
      match json |> member "$type" |> to_string with
      | "com.atproto.repo.applyWrites#create" -> (
        match create_of_yojson json with
        | Ok v ->
            Ok (Create v)
        | Error e ->
            Error e )
      | "com.atproto.repo.applyWrites#update" -> (
        match update_of_yojson json with
        | Ok v ->
            Ok (Update v)
        | Error e ->
            Error e )
      | "com.atproto.repo.applyWrites#delete" -> (
        match delete_of_yojson json with
        | Ok v ->
            Ok (Delete v)
        | Error e ->
            Error e )
      | t ->
          Error ("unknown union type: " ^ t)
    with _ -> Error "failed to parse union"

  let writes_item_to_yojson = function
    | Create v -> (
      match create_to_yojson v with
      | `Assoc fields ->
          `Assoc
            (("$type", `String "com.atproto.repo.applyWrites#create") :: fields)
      | other ->
          other )
    | Update v -> (
      match update_to_yojson v with
      | `Assoc fields ->
          `Assoc
            (("$type", `String "com.atproto.repo.applyWrites#update") :: fields)
      | other ->
          other )
    | Delete v -> (
      match delete_to_yojson v with
      | `Assoc fields ->
          `Assoc
            (("$type", `String "com.atproto.repo.applyWrites#delete") :: fields)
      | other ->
          other )

  type input =
    { repo: string
    ; validate: bool option [@default None]
    ; writes: writes_item list
    ; swap_commit: string option [@key "swapCommit"] [@default None] }
  [@@deriving yojson {strict= false}]

  type results_item =
    | CreateResult of create_result
    | UpdateResult of update_result
    | DeleteResult of delete_result

  let results_item_of_yojson json =
    let open Yojson.Safe.Util in
    try
      match json |> member "$type" |> to_string with
      | "com.atproto.repo.applyWrites#createResult" -> (
        match create_result_of_yojson json with
        | Ok v ->
            Ok (CreateResult v)
        | Error e ->
            Error e )
      | "com.atproto.repo.applyWrites#updateResult" -> (
        match update_result_of_yojson json with
        | Ok v ->
            Ok (UpdateResult v)
        | Error e ->
            Error e )
      | "com.atproto.repo.applyWrites#deleteResult" -> (
        match delete_result_of_yojson json with
        | Ok v ->
            Ok (DeleteResult v)
        | Error e ->
            Error e )
      | t ->
          Error ("unknown union type: " ^ t)
    with _ -> Error "failed to parse union"

  let results_item_to_yojson = function
    | CreateResult v -> (
      match create_result_to_yojson v with
      | `Assoc fields ->
          `Assoc
            ( ("$type", `String "com.atproto.repo.applyWrites#createResult")
            :: fields )
      | other ->
          other )
    | UpdateResult v -> (
      match update_result_to_yojson v with
      | `Assoc fields ->
          `Assoc
            ( ("$type", `String "com.atproto.repo.applyWrites#updateResult")
            :: fields )
      | other ->
          other )
    | DeleteResult v -> (
      match delete_result_to_yojson v with
      | `Assoc fields ->
          `Assoc
            ( ("$type", `String "com.atproto.repo.applyWrites#deleteResult")
            :: fields )
      | other ->
          other )

  type output =
    { commit: Com_atproto_repo_defs.commit_meta option [@default None]
    ; results: results_item list option [@default None] }
  [@@deriving yojson {strict= false}]

  let call ~repo ?validate ~writes ?swap_commit (client : Hermes.client) :
      output Lwt.t =
    let params = () in
    let input =
      Some ({repo; validate; writes; swap_commit} |> input_to_yojson)
    in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
