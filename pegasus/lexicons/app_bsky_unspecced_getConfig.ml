(* generated from app.bsky.unspecced.getConfig *)

type live_now_config =
  {
    did: string;
    domains: string list;
  }
[@@deriving yojson {strict= false}]

(** Get miscellaneous runtime configuration. *)
module Main = struct
  let nsid = "app.bsky.unspecced.getConfig"

  type params = unit
  let params_to_yojson () = `Assoc []

  type output =
  {
    check_email_confirmed: bool option [@key "checkEmailConfirmed"] [@default None];
    live_now: live_now_config list option [@key "liveNow"] [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end

