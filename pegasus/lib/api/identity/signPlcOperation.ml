type request =
  { token: string
  ; rotation_keys: string list option [@default None]
  ; verification_methods: Util.Did_doc_types.string_map option [@default None]
  ; also_known_as: string list option [@default None]
  ; services: Plc.service_map option [@default None] }
[@@deriving yojson {strict= false}]

type response = {operation: Plc.signed_operation}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Authorization (fun {req; auth; db; _} ->
      let did = Auth.get_authed_did_exn auth in
      Auth.assert_identity_scope auth ~attr:Oauth.Scopes.Any ;
      if not (String.starts_with ~prefix:"did:plc:" did) then
        Errors.invalid_request "this method is only for did:plc identities" ;
      let%lwt input = Xrpc.parse_body req request_of_yojson in
      match%lwt Data_store.get_actor_by_identifier did db with
      | None ->
          Errors.internal_error ~msg:"actor not found" ()
      | Some actor -> (
        match (actor.auth_code, actor.auth_code_expires_at) with
        | Some auth_code, Some auth_expires_at
          when String.starts_with ~prefix:"plc-" auth_code
               && input.token = auth_code
               && Util.now_ms () < auth_expires_at -> (
          match%lwt Plc.get_audit_log did with
          | Ok log ->
              let latest = Mist.Util.last log |> Option.get in
              let unsigned_op : Plc.unsigned_operation =
                Operation
                  { type'= "plc_operation"
                  ; rotation_keys=
                      Option.value input.rotation_keys
                        ~default:latest.operation.rotation_keys
                  ; verification_methods=
                      Option.value input.verification_methods
                        ~default:latest.operation.verification_methods
                  ; also_known_as=
                      Option.value input.also_known_as
                        ~default:latest.operation.also_known_as
                  ; services=
                      Option.value input.services
                        ~default:latest.operation.services
                  ; prev= Some latest.cid }
              in
              let signed_op = Plc.sign_operation Env.rotation_key unsigned_op in
              let%lwt () = Data_store.clear_auth_code ~did db in
              let res = {operation= signed_op} in
              res |> response_to_yojson |> Yojson.Safe.to_string
              |> Dream.json ~status:`OK
          | Error err ->
              Errors.internal_error ~msg:("failed to get audit log: " ^ err) ()
          )
        | _ ->
            Errors.invalid_request ~name:"InvalidToken"
              "token expired or invalid" ) )
