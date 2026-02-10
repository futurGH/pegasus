(* generated from app.bsky.feed.describeFeedGenerator *)

type links =
  { privacy_policy: string option [@key "privacyPolicy"] [@default None]
  ; terms_of_service: string option [@key "termsOfService"] [@default None] }
[@@deriving yojson {strict= false}]

type feed = {uri: string} [@@deriving yojson {strict= false}]

(** Get information about a feed generator, including policies and offered feed URIs. Does not require auth; implemented by Feed Generator services (not App View). *)
module Main = struct
  let nsid = "app.bsky.feed.describeFeedGenerator"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output =
    {did: string; feeds: feed list; links: links option [@default None]}
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
