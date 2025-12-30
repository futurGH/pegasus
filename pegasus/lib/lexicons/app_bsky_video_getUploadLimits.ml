(* generated from app.bsky.video.getUploadLimits *)

(** Get video upload limits for the authenticated user. *)
module Main = struct
  let nsid = "app.bsky.video.getUploadLimits"

  type params = unit
  let params_to_yojson () = `Assoc []

  type output =
  {
    can_upload: bool [@key "canUpload"];
    remaining_daily_videos: int option [@key "remainingDailyVideos"] [@default None];
    remaining_daily_bytes: int option [@key "remainingDailyBytes"] [@default None];
    message: string option [@default None];
    error: string option [@default None];
  }
[@@deriving yojson {strict= false}]

  let call
      (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end

