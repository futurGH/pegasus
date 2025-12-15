type response = {preferences: Yojson.Safe.t} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Authorization (fun {db; auth; _} ->
      let did = Auth.get_authed_did_exn auth in
      let%lwt preferences =
        match%lwt Data_store.get_actor_by_identifier did db with
        | Some actor ->
            Lwt.return actor.preferences
        | None ->
            Errors.internal_error ()
      in
      {preferences} |> response_to_yojson |> Yojson.Safe.to_string |> Dream.json )
