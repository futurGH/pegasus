(* generated from com.atproto.repo.describeRepo *)

(** Get information about an account and repository, including the list of collections. Does not require auth. *)
module Main = struct
  let nsid = "com.atproto.repo.describeRepo"

  type params =
  {
    repo: string;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    handle: string;
    did: string;
    did_doc: Yojson.Safe.t [@key "didDoc"];
    collections: string list;
    handle_is_correct: bool [@key "handleIsCorrect"];
  }
[@@deriving yojson {strict= false}]

  let call
      ~repo
      (client : Hermes.client) : output Lwt.t =
    let params : params = {repo} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

