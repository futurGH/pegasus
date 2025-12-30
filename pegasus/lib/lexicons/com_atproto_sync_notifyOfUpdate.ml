(* generated from com.atproto.sync.notifyOfUpdate *)

(** Notify a crawling service of a recent update, and that crawling should resume. Intended use is after a gap between repo stream events caused the crawling service to disconnect. Does not require auth; implemented by Relay. DEPRECATED: just use com.atproto.sync.requestCrawl *)
module Main = struct
  let nsid = "com.atproto.sync.notifyOfUpdate"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      hostname: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
  let output_of_yojson _ = Ok ()

  let call
      ~hostname
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({hostname} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

