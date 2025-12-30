(* generated from app.bsky.video.getJobStatus *)

(** Get status details for a video processing job. *)
module Main = struct
  let nsid = "app.bsky.video.getJobStatus"

  type params =
  {
    job_id: string [@key "jobId"];
  }
[@@deriving yojson {strict= false}]

  type output =
  {
    job_status: App_bsky_video_defs.job_status [@key "jobStatus"];
  }
[@@deriving yojson {strict= false}]

  let call
      ~job_id
      (client : Hermes.client) : output Lwt.t =
    let params : params = {job_id} in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end

