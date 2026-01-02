type setup_response = {secret: string; uri: string}
[@@deriving yojson {strict= false}]

type verify_request = {code: string} [@@deriving yojson {strict= false}]

type verify_response =
  {success: bool; backup_codes: string list option [@default None]}
[@@deriving yojson {strict= false}]

type success_response = {success: bool; message: string option [@default None]}
[@@deriving yojson {strict= false}]

let setup_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      match%lwt Data_store.get_actor_by_identifier did ctx.db with
      | None ->
          Errors.auth_required "user not found"
      | Some actor ->
          let%lwt already_enabled = Totp.is_enabled ~did ctx.db in
          if already_enabled then
            Errors.invalid_request "TOTP is already enabled"
          else
            let secret = Totp.generate_secret () in
            let issuer = "Pegasus PDS (" ^ Env.hostname ^ ")" in
            let uri =
              Totp.make_provisioning_uri ~secret ~email:actor.email ~issuer
            in
            let secret_b32 =
              Multibase.Base32.encode_exn ~pad:false (Bytes.to_string secret)
            in
            let%lwt () = Totp.create_secret ~did ~secret ctx.db in
            Dream.json @@ Yojson.Safe.to_string
            @@ setup_response_to_yojson {secret= secret_b32; uri} )

let verify_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      let%lwt {code; _} = Xrpc.parse_body ctx.req verify_request_of_yojson in
      match%lwt Totp.verify_and_enable ~did ~code ctx.db with
      | Error msg ->
          Errors.invalid_request msg
      | Ok () ->
          let%lwt backup_codes =
            Totp.Backup_codes.ensure_codes_exist ~did ctx.db
          in
          Dream.json @@ Yojson.Safe.to_string
          @@ verify_response_to_yojson {success= true; backup_codes} )

let disable_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      let%lwt () = Totp.disable ~did ctx.db in
      Dream.json @@ Yojson.Safe.to_string
      @@ success_response_to_yojson
           {success= true; message= Some "TOTP disabled"} )
