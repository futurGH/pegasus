open Lexicons.Com.Atproto.Identity.SignPlcOperation.Main

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      Auth.assert_identity_scope auth ~attr:Oauth.Scopes.Any ;
      if not (String.starts_with ~prefix:"did:plc:" did) then
        Errors.invalid_request "this method is only for did:plc identities" ;
      let%lwt input = Xrpc.parse_body req input_of_yojson in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor -> (
        match (actor.auth_code, actor.auth_code_expires_at) with
        | auth_code, Some auth_expires_at
          when input.token = auth_code && Util.Time.now_ms () < auth_expires_at
          -> (
          match%lwt Plc.get_audit_log did with
          | Ok log ->
              let latest = Mist.Util.last log |> Option.get in
              let input_vm =
                Option.map
                  (fun v ->
                    try Util.Types.string_map_of_yojson v |> Result.get_ok
                    with _ -> Errors.invalid_request "invalid request body" )
                  input.verification_methods
              in
              let input_svcs =
                Option.map
                  (fun v ->
                    try Plc.service_map_of_yojson v |> Result.get_ok
                    with _ -> Errors.invalid_request "invalid request body" )
                  input.services
              in
              let unsigned_op : Plc.unsigned_operation =
                Operation
                  { type'= "plc_operation"
                  ; rotation_keys=
                      Option.value input.rotation_keys
                        ~default:latest.operation.rotation_keys
                  ; verification_methods=
                      Option.value input_vm
                        ~default:latest.operation.verification_methods
                  ; also_known_as=
                      Option.value input.also_known_as
                        ~default:latest.operation.also_known_as
                  ; services=
                      Option.value input_svcs ~default:latest.operation.services
                  ; prev= Some latest.cid }
              in
              let signed_op =
                Plc.sign_operation Env.rotation_key unsigned_op
                |> Plc.signed_operation_to_yojson
              in
              let%lwt () = Data_store.clear_auth_code ~did db in
              output_to_yojson {operation= signed_op}
              |> Yojson.Safe.to_string |> Dream.json ~status:`OK
          | Error err ->
              Errors.internal_error ~msg:("failed to get audit log: " ^ err) ()
          )
        | _ ->
            Errors.invalid_request ~name:"InvalidToken"
              "token expired or invalid" ) )
