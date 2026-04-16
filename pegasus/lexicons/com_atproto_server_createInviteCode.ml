(* generated from com.atproto.server.createInviteCode *)

(** Create an invite code. *)
module Main = struct
  let nsid = "com.atproto.server.createInviteCode"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      use_count: int [@key "useCount"];
      for_account: string option [@key "forAccount"] [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    code: string;
  }
[@@deriving yojson {strict= false}]

  let call
      ~use_count
      ?for_account
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({use_count; for_account} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

