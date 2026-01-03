(* generated from com.atproto.lexicon.resolveLexicon *)

(** Resolves an atproto lexicon (NSID) to a schema. *)
module Main = struct
  let nsid = "com.atproto.lexicon.resolveLexicon"

  type params =
  {
    nsid: string;
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    cid: string;
    schema: Com_atproto_lexicon_schema.main;
    uri: string;
  }
[@@deriving yojson {strict= false}]

  let call
      ~nsid
      (client : Hermes.client) : output Lwt.t =
    let params : params = {nsid} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

