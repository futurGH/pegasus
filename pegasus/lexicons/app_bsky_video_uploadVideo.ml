(* generated from app.bsky.video.uploadVideo *)

(** Upload a video to be processed then stored on the PDS. *)
module Main = struct
  let nsid = "app.bsky.video.uploadVideo"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output = {job_status: App_bsky_video_defs.job_status [@key "jobStatus"]}
  [@@deriving yojson {strict= false}]

  let call ?input (client : Hermes.client) : output Lwt.t =
    let params = () in
    Hermes.procedure_blob client nsid (params_to_yojson params)
      (Bytes.of_string (Option.value input ~default:""))
      ~content_type:"video/mp4" output_of_yojson
end
