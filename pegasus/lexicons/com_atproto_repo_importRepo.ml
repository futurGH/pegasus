(* generated from com.atproto.repo.importRepo *)

(** Import a repo in the form of a CAR file. Requires Content-Length HTTP header to be set. *)
module Main = struct
  let nsid = "com.atproto.repo.importRepo"

  type params = unit

  let params_to_yojson () = `Assoc []

  type output = unit

  let output_of_yojson _ = Ok ()

  let call ?input (client : Hermes.client) : output Lwt.t =
    let params = () in
    let open Lwt.Syntax in
    let* _ =
      Hermes.procedure_bytes client nsid (params_to_yojson params) input
        ~content_type:"application/vnd.ipld.car"
    in
    Lwt.return ()
end
