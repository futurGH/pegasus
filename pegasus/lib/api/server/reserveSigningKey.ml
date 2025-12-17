type request = {did: string option [@default None]}
[@@deriving yojson {strict= false}]

type response = {signing_key: string [@key "signingKey"]} [@@deriving yojson]

let handler =
  Xrpc.handler (fun {req; db; _} ->
      let%lwt {did} = Xrpc.parse_body req request_of_yojson in
      let%lwt existing =
        match did with
        | Some did when did <> "" ->
            Data_store.get_reserved_key_by_did ~did db
        | _ ->
            Lwt.return_none
      in
      match existing with
      | Some key ->
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson {signing_key= key.key_did}
      | None ->
          let privkey, pubkey = Kleidos.K256.generate_keypair () in
          let key_did = Kleidos.K256.pubkey_to_did_key pubkey in
          let private_key = Kleidos.K256.privkey_to_multikey privkey in
          let%lwt () =
            Data_store.create_reserved_key ~key_did ~did ~private_key db
          in
          Dream.json @@ Yojson.Safe.to_string
          @@ response_to_yojson {signing_key= key_did} )
