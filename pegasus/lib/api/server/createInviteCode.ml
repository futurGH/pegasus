open Lexicons.Com_atproto_server_createInviteCode.Main

let generate_code did =
  String.sub
    Digestif.SHA256.(digest_string (did ^ Mist.Tid.now ()) |> to_hex)
    0 8

let create_invite_code ~db ~did ~use_count =
  let remaining = Int.max 1 (Int.min use_count 5) in
  let code = generate_code did in
  let%lwt () = Data_store.create_invite ~code ~did ~remaining db in
  Lwt.return code

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {use_count; for_account} =
        Xrpc.parse_body req input_of_yojson
      in
      let%lwt code =
        create_invite_code ~db
          ~did:(Option.value for_account ~default:"admin")
          ~use_count
      in
      Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson {code} )
