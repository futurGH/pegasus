type passkey_info =
  { id: int
  ; name: string
  ; created_at: int
  ; last_used_at: int option [@default None] }
[@@deriving yojson {strict= false}]

type list_response = {passkeys: passkey_info list}
[@@deriving yojson {strict= false}]

type register_request =
  {name: string [@default "Passkey"]; response: string; challenge: string}
[@@deriving yojson {strict= false}]

type auth_request = {response: string; challenge: string}
[@@deriving yojson {strict= false}]

type success_response = {success: bool; message: string option [@default None]}
[@@deriving yojson {strict= false}]

type login_success_response = {success: bool; redirect: string}
[@@deriving yojson {strict= false}]

type error_response = {error: string} [@@deriving yojson {strict= false}]

(* helper to get the current DID or return unauthorized *)
let with_current_did ctx f =
  match%lwt Session.Raw.get_current_did ctx.Xrpc.req with
  | None ->
      Errors.auth_required "not authorized"
  | Some did ->
      f did

let list_handler =
  Xrpc.handler (fun ctx ->
      with_current_did ctx (fun did ->
          let%lwt passkeys = Passkey.get_credentials_for_user ~did ctx.db in
          let passkey_infos =
            List.map
              (fun (pk : Passkey.Types.passkey) ->
                { id= pk.id
                ; name= pk.name
                ; created_at= pk.created_at
                ; last_used_at= pk.last_used_at } )
              passkeys
          in
          Dream.json @@ Yojson.Safe.to_string
          @@ list_response_to_yojson {passkeys= passkey_infos} ) )

let register_options_handler =
  Xrpc.handler (fun ctx ->
      with_current_did ctx (fun did ->
          match%lwt Data_store.get_actor_by_identifier did ctx.db with
          | None ->
              Errors.auth_required "user not found"
          | Some actor ->
              let%lwt existing = Passkey.get_credentials_for_user ~did ctx.db in
              let%lwt options =
                Passkey.generate_registration_options ~did ~email:actor.email
                  ~existing_credentials:existing ctx.db
              in
              Dream.json @@ Yojson.Safe.to_string options ) )

let register_verify_handler =
  Xrpc.handler (fun ctx ->
      with_current_did ctx (fun did ->
          let%lwt {challenge; response; name; _} =
            Xrpc.parse_body ctx.req register_request_of_yojson
          in
          match%lwt Passkey.verify_registration ~challenge ~response ctx.db with
          | Error msg ->
              Errors.invalid_request msg
          | Ok (credential_id, public_key) ->
              let%lwt () =
                Passkey.store_credential ~did ~credential_id ~public_key ~name
                  ctx.db
              in
              Dream.json @@ Yojson.Safe.to_string
              @@ success_response_to_yojson
                   {success= true; message= Some "Passkey registered"} ) )

let login_options_handler =
  Xrpc.handler (fun ctx ->
      let%lwt options = Passkey.generate_authentication_options ctx.db in
      Dream.json @@ Yojson.Safe.to_string options )

let login_verify_handler =
  Xrpc.handler (fun ctx ->
      let%lwt {challenge; response; _} =
        Xrpc.parse_body ctx.req auth_request_of_yojson
      in
      match%lwt Passkey.verify_authentication ~challenge ~response ctx.db with
      | Error msg ->
          Errors.auth_required msg
      | Ok did ->
          let%lwt () = Session.log_in_did ctx.req did in
          let redirect_url =
            Dream.query ctx.req "redirect_url"
            |> Option.value ~default:"/account"
          in
          Dream.json @@ Yojson.Safe.to_string
          @@ login_success_response_to_yojson
               {success= true; redirect= redirect_url} )

let delete_handler =
  Xrpc.handler (fun ctx ->
      with_current_did ctx (fun did ->
          let id_str = Dream.param ctx.req "id" in
          match int_of_string_opt id_str with
          | None ->
              Errors.invalid_request "invalid passkey id"
          | Some id ->
              let%lwt _ = Passkey.delete_credential ~id ~did ctx.db in
              Dream.json @@ Yojson.Safe.to_string
              @@ success_response_to_yojson
                   {success= true; message= Some "Passkey deleted"} ) )

let rename_handler =
  Xrpc.handler (fun ctx ->
      with_current_did ctx (fun did ->
          let id_str = Dream.param ctx.req "id" in
          match int_of_string_opt id_str with
          | None ->
              Errors.invalid_request "invalid passkey id"
          | Some id -> (
              let%lwt body = Dream.body ctx.req in
              let json = Yojson.Safe.from_string body in
              match Yojson.Safe.Util.member "name" json with
              | `String name ->
                  let%lwt _success =
                    Passkey.rename_credential ~id ~did ~name ctx.db
                  in
                  Dream.json @@ Yojson.Safe.to_string
                  @@ success_response_to_yojson {success= true; message= None}
              | _ ->
                  Errors.invalid_request "missing name field" ) ) )
