(* generated from com.atproto.server.checkAccountStatus *)

(** Returns the status of an account, especially as pertaining to import or recovery. Can be called many times over the course of an account migration. Requires auth and can only be called pertaining to oneself. *)
module Main = struct
  let nsid = "com.atproto.server.checkAccountStatus"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output =
    { activated: bool
    ; valid_did: bool [@key "validDid"]
    ; repo_commit: string [@key "repoCommit"]
    ; repo_rev: string [@key "repoRev"]
    ; repo_blocks: int [@key "repoBlocks"]
    ; indexed_records: int [@key "indexedRecords"]
    ; private_state_values: int [@key "privateStateValues"]
    ; expected_blobs: int [@key "expectedBlobs"]
    ; imported_blobs: int [@key "importedBlobs"] }
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
