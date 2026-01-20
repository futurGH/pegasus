(* generated from app.bsky.unspecced.getOnboardingSuggestedStarterPacksSkeleton *)

(** Get a skeleton of suggested starterpacks for onboarding. Intended to be called and hydrated by app.bsky.unspecced.getOnboardingSuggestedStarterPacks *)
module Main = struct
  let nsid = "app.bsky.unspecced.getOnboardingSuggestedStarterPacksSkeleton"

  type params =
    {viewer: string option [@default None]; limit: int option [@default None]}
  [@@deriving yojson {strict= false}]

  type output = {starter_packs: string list [@key "starterPacks"]}
  [@@deriving yojson {strict= false}]

  let call ?viewer ?limit (client : Hermes.client) : output Lwt.t =
    let params : params = {viewer; limit} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
