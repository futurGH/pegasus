(* generated from com.atproto.moderation.defs *)

(** string type with known values *)
type reason_type = string
let reason_type_of_yojson = function
  | `String s -> Ok s
  | _ -> Error "reason_type: expected string"
let reason_type_to_yojson s = `String s

(** Spam: frequent unwanted promotion, replies, mentions *)
let reason_spam = "com.atproto.moderation.defs#reasonSpam"

(** Direct violation of server rules, laws, terms of service *)
let reason_violation = "com.atproto.moderation.defs#reasonViolation"

(** Misleading identity, affiliation, or content *)
let reason_misleading = "com.atproto.moderation.defs#reasonMisleading"

(** Unwanted or mislabeled sexual content *)
let reason_sexual = "com.atproto.moderation.defs#reasonSexual"

(** Rude, harassing, explicit, or otherwise unwelcoming behavior *)
let reason_rude = "com.atproto.moderation.defs#reasonRude"

(** Other: reports not falling under another report category *)
let reason_other = "com.atproto.moderation.defs#reasonOther"

(** Appeal: appeal a previously taken moderation action *)
let reason_appeal = "com.atproto.moderation.defs#reasonAppeal"

(** string type with known values: Tag describing a type of subject that might be reported. *)
type subject_type = string
let subject_type_of_yojson = function
  | `String s -> Ok s
  | _ -> Error "subject_type: expected string"
let subject_type_to_yojson s = `String s

