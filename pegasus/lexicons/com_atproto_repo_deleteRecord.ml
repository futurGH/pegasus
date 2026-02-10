(* generated from com.atproto.repo.deleteRecord *)

(** Delete a repository record, or ensure it doesn't exist. Requires auth, implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.repo.deleteRecord"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input =
    { repo: string
    ; collection: string
    ; rkey: string
    ; swap_record: string option [@key "swapRecord"] [@default None]
    ; swap_commit: string option [@key "swapCommit"] [@default None] }
  [@@deriving yojson {strict= false}]

  type output =
    {commit: Com_atproto_repo_defs.commit_meta option [@default None]}
  [@@deriving yojson {strict= false}]

  let call ~repo ~collection ~rkey ?swap_record ?swap_commit
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input =
      Some
        ({repo; collection; rkey; swap_record; swap_commit} |> input_to_yojson)
    in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
