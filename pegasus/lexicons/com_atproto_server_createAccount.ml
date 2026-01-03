(* generated from com.atproto.server.createAccount *)

(** Create an account. Implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.server.createAccount"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      email: string option [@default None];
      handle: string;
      did: string option [@default None];
      invite_code: string option [@key "inviteCode"] [@default None];
      verification_code: string option [@key "verificationCode"] [@default None];
      verification_phone: string option [@key "verificationPhone"] [@default None];
      password: string option [@default None];
      recovery_key: string option [@key "recoveryKey"] [@default None];
      plc_op: Yojson.Safe.t option [@key "plcOp"] [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    access_jwt: string [@key "accessJwt"];
    refresh_jwt: string [@key "refreshJwt"];
    handle: string;
    did: string;
    did_doc: Yojson.Safe.t option [@key "didDoc"] [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ?email
      ~handle
      ?did
      ?invite_code
      ?verification_code
      ?verification_phone
      ?password
      ?recovery_key
      ?plc_op
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({email; handle; did; invite_code; verification_code; verification_phone; password; recovery_key; plc_op} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

