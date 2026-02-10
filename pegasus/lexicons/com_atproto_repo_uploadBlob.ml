(* generated from com.atproto.repo.uploadBlob *)

(** Upload a new blob, to be referenced from a repository record. The blob will be deleted if it is not referenced within a time window (eg, minutes). Blob restrictions (mimetype, size, etc) are enforced when the reference is created. Requires auth, implemented by PDS. *)
module Main = struct
  let nsid = "com.atproto.repo.uploadBlob"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output = {blob: Hermes.blob} [@@deriving yojson {strict= false}]

  let call ?input (client : Hermes.client) : output Lwt.t =
    let params = () in
    Hermes.procedure_blob client nsid (params_to_yojson params)
      (Bytes.of_string (Option.value input ~default:""))
      ~content_type:"*/*" output_of_yojson
end
