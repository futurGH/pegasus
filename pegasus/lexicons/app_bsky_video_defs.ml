(* generated from app.bsky.video.defs *)

type job_status =
  { job_id: string [@key "jobId"]
  ; did: string
  ; state: string
  ; progress: int option [@default None]
  ; blob: Hermes.blob option [@default None]
  ; error: string option [@default None]
  ; message: string option [@default None] }
[@@deriving yojson {strict= false}]
