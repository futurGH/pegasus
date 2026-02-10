(* generated from com.atproto.identity.getRecommendedDidCredentials *)

(** Describe the credentials that should be included in the DID doc of an account that is migrating to this service. *)
module Main = struct
  let nsid = "com.atproto.identity.getRecommendedDidCredentials"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output =
    { rotation_keys: string list option [@key "rotationKeys"] [@default None]
    ; also_known_as: string list option [@key "alsoKnownAs"] [@default None]
    ; verification_methods: Yojson.Safe.t option
          [@key "verificationMethods"] [@default None]
    ; services: Yojson.Safe.t option [@default None] }
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
