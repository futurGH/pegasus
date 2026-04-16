(* generated from app.bsky.draft.createDraft *)

(** Inserts a draft using private storage (stash). An upper limit of drafts might be enforced. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.draft.createDraft"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      draft: App_bsky_draft_defs.draft;
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    id: string;
  }
[@@deriving yojson {strict= false}]

  let call
      ~draft
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({draft} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

