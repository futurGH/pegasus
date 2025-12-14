type request =
  { use_count: int [@key "useCount"]
  ; for_account: string option [@key "forAccount"] [@default None] }
[@@deriving yojson]

type response = {code: string} [@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {use_count; for_account} =
        Xrpc.parse_body req request_of_yojson
      in
      let remaining = Int.max 1 (Int.min use_count 5) in
      let did = Option.value for_account ~default:"admin" in
      let code =
        String.sub
          Digestif.SHA256.(
            digest_string (did ^ Int.to_string @@ Util.now_ms ()) |> to_hex )
          0 8
      in
      let%lwt () = Data_store.create_invite ~code ~did ~remaining db in
      Dream.json @@ Yojson.Safe.to_string @@ response_to_yojson {code} )
