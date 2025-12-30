(* generated from com.atproto.server.describeServer *)

type contact =
  {
    email: string option [@default None];
  }
[@@deriving yojson {strict= false}]

type links =
  {
    privacy_policy: string option [@key "privacyPolicy"] [@default None];
    terms_of_service: string option [@key "termsOfService"] [@default None];
  }
[@@deriving yojson {strict= false}]

(** Describes the server's account creation requirements and capabilities. Implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.server.describeServer"

  type params = unit
  let params_to_yojson () = `Assoc []

  type output =
  {
    invite_code_required: bool option [@key "inviteCodeRequired"] [@default None];
    phone_verification_required: bool option [@key "phoneVerificationRequired"] [@default None];
    available_user_domains: string list [@key "availableUserDomains"];
    links: links option [@default None];
    contact: contact option [@default None];
    did: string;
  }
[@@deriving yojson {strict= false}]

  let call
      (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end

