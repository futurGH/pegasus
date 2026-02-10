(* generated from app.bsky.unspecced.getTaggedSuggestions *)

type suggestion =
  {tag: string; subject_type: string [@key "subjectType"]; subject: string}
[@@deriving yojson {strict= false}]

(** Get a list of suggestions (feeds and users) tagged with categories *)
module Main = struct
  let nsid = "app.bsky.unspecced.getTaggedSuggestions"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output = {suggestions: suggestion list}
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
