(* generated from app.bsky.feed.sendInteractions *)

(** Send information about interactions with feed items back to the feed generator that served them. *)
module Main = struct
  let nsid = "app.bsky.feed.sendInteractions"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      interactions: App_bsky_feed_defs.interaction list;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
let output_of_yojson _ = Ok ()
let output_to_yojson () = `Assoc []

  let call
      ~interactions
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({interactions} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

