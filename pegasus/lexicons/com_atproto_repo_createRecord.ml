(* generated from com.atproto.repo.createRecord *)

(** Create a single new repository record. Requires auth, implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.repo.createRecord"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      repo: string;
      collection: string;
      rkey: string option [@default None];
      validate: bool option [@default None];
      record: Yojson.Safe.t;
      swap_commit: string option [@key "swapCommit"] [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    uri: string;
    cid: string;
    commit: Com_atproto_repo_defs.commit_meta option [@default None];
    validation_status: string option [@key "validationStatus"] [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ~repo
      ~collection
      ?rkey
      ?validate
      ~record
      ?swap_commit
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({repo; collection; rkey; validate; record; swap_commit} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

