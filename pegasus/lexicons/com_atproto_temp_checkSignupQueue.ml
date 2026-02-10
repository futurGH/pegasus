(* generated from com.atproto.temp.checkSignupQueue *)

(** Check accounts location in signup queue. *)
module Main = struct
  let nsid = "com.atproto.temp.checkSignupQueue"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output =
    { activated: bool
    ; place_in_queue: int option [@key "placeInQueue"] [@default None]
    ; estimated_time_ms: int option [@key "estimatedTimeMs"] [@default None] }
  [@@deriving yojson {strict= false}]

  let call (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end
