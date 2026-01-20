(* generated from app.bsky.bookmark.createBookmark *)

(** Creates a private bookmark for the specified record. Currently, only `app.bsky.feed.post` records are supported. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.bookmark.createBookmark"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {uri: string; cid: string} [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~uri ~cid (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({uri; cid} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
