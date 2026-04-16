(* generated from app.bsky.draft.deleteDraft *)

(** Deletes a draft by ID. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.draft.deleteDraft"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      id: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~id
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({id} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

