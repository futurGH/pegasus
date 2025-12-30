(* generated from com.atproto.server.createSession *)

(** Create an authentication session. *)
module Main = struct
  let nsid = "com.atproto.server.createSession"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      identifier: string;
      password: string;
      auth_factor_token: string option [@key "authFactorToken"] [@default None];
      allow_takendown: bool option [@key "allowTakendown"] [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    access_jwt: string [@key "accessJwt"];
    refresh_jwt: string [@key "refreshJwt"];
    handle: string;
    did: string;
    did_doc: Yojson.Safe.t option [@key "didDoc"] [@default None];
    email: string option [@default None];
    email_confirmed: bool option [@key "emailConfirmed"] [@default None];
    email_auth_factor: bool option [@key "emailAuthFactor"] [@default None];
    active: bool option [@default None];
    status: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      ~identifier
      ~password
      ?auth_factor_token
      ?allow_takendown
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({identifier; password; auth_factor_token; allow_takendown} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

