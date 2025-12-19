type response = {infos: GetAccountInfo.account_view list}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let dids =
        Dream.query req "dids" |> Option.value ~default:""
        |> String.split_on_char ','
        |> List.filter (fun s -> String.length s > 0)
      in
      let%lwt infos =
        Lwt_list.filter_map_s
          (fun did ->
            match%lwt Data_store.get_actor_by_identifier did db with
            | None ->
                Lwt.return_none
            | Some actor ->
                Lwt.return_some (GetAccountInfo.actor_to_account_view actor) )
          dids
      in
      Dream.json @@ Yojson.Safe.to_string @@ response_to_yojson {infos} )
