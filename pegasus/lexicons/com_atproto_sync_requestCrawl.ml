(* generated from com.atproto.sync.requestCrawl *)

(** Request a service to persistently crawl hosted repos. Expected use is new PDS instances declaring their existence to Relays. Does not require auth. *)
module Main = struct
  let nsid = "com.atproto.sync.requestCrawl"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {hostname: string} [@@deriving yojson {strict= false}]

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ~hostname (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({hostname} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
