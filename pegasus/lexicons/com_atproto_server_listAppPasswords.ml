(* generated from com.atproto.server.listAppPasswords *)

type app_password =
  {
    name: string;
    created_at: string [@key "createdAt"];
    privileged: bool option [@default None];
  }
[@@deriving yojson {strict= false}]

(** List all App Passwords. *)
module Main = struct
  let nsid = "com.atproto.server.listAppPasswords"

  type params = unit
  let params_to_yojson () = `Assoc []

  type output =
  {
    passwords: app_password list;
  }
[@@deriving yojson {strict= false}]

  let call
      (client : Hermes.client) : output Lwt.t =
    Hermes.query client nsid (`Assoc []) output_of_yojson
end

