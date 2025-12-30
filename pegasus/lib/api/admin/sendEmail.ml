open Lexicons.Com_atproto_admin_sendEmail.Main

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let%lwt {recipient_did; content; subject; sender_did; comment= _} =
        Xrpc.parse_body req input_of_yojson
      in
      match%lwt Data_store.get_actor_by_identifier recipient_did db with
      | None ->
          Errors.invalid_request "recipient account not found"
      | Some recipient -> (
        match%lwt Data_store.get_actor_by_identifier sender_did db with
        | None ->
            Errors.invalid_request "sender account not found"
        | Some sender ->
            let subject =
              Option.value subject
                ~default:
                  (Printf.sprintf "Message from %s via %s" sender.handle
                     Env.hostname )
            in
            let%lwt () =
              Util.send_email_or_log ~recipients:[To recipient.email] ~subject
                ~body:(Plain content)
            in
            Dream.json @@ Yojson.Safe.to_string @@ output_to_yojson {sent= true}
        ) )
