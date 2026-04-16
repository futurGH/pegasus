(* generated from app.bsky.draft.updateDraft *)

(** Updates a draft using private storage (stash). If the draft ID points to a non-existing ID, the update will be silently ignored. This is done because updates don't enforce draft limit, so it accepts all writes, but will ignore invalid ones. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.draft.updateDraft"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      draft: App_bsky_draft_defs.draft_with_id;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~draft
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({draft} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

