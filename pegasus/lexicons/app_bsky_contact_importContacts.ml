(* generated from app.bsky.contact.importContacts *)

(** Import contacts for securely matching with other users. This follows the protocol explained in https://docs.bsky.app/blog/contact-import-rfc. Requires authentication. *)
module Main = struct
  let nsid = "app.bsky.contact.importContacts"

  type params = unit

  let params_to_yojson () = `Assoc []

  type input = {token: string; contacts: string list}
  [@@deriving yojson {strict= false}]

  type output =
    { matches_and_contact_indexes:
        App_bsky_contact_defs.match_and_contact_index list
          [@key "matchesAndContactIndexes"] }
  [@@deriving yojson {strict= false}]

  let call ~token ~contacts (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({token; contacts} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input
      output_of_yojson
end
