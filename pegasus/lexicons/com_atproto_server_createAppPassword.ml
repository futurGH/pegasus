(* generated from com.atproto.server.createAppPassword *)

type app_password =
  {
    name: string;
    password: string;
    created_at: string [@key "createdAt"];
    privileged: bool option [@default None];
  }
[@@deriving yojson {strict= false}]

(** Create an App Password. *)
module Main = struct
  let nsid = "com.atproto.server.createAppPassword"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      name: string;
      privileged: bool option [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output = app_password
[@@deriving yojson {strict= false}]

  let call
      ~name
      ?privileged
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({name; privileged} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

