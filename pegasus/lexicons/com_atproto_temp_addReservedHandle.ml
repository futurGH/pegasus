(* generated from com.atproto.temp.addReservedHandle *)

(** Add a handle to the set of reserved handles. *)
module Main = struct
  let nsid = "com.atproto.temp.addReservedHandle"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      handle: string;
    }
  [@@deriving yojson {strict= false}]

  type output = unit
let output_of_yojson _ = Ok ()
let output_to_yojson () = `Assoc []

  let call
      ~handle
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({handle} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

