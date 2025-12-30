(* generated from com.atproto.temp.dereferenceScope *)

(** Allows finding the oauth permission scope from a reference *)
module Main = struct
  let nsid = "com.atproto.temp.dereferenceScope"

  type params =
  {
    scope: string;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    scope: string;
  }
[@@deriving yojson {strict= false}]

  let call
      ~scope
      (client : Hermes.client) : output Lwt.t =
    let params : params = {scope} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

