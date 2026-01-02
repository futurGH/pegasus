type security_key_display =
  { id: int
  ; name: string
  ; created_at: int
  ; last_used_at: int option [@default None]
  ; verified: bool }
[@@deriving yojson {strict= false}]

type list_response = {security_keys: security_key_display list}
[@@deriving yojson {strict= false}]

type setup_request = {name: string option [@default None]}
[@@deriving yojson {strict= false}]

type setup_response = {id: int; secret: string; uri: string}
[@@deriving yojson {strict= false}]

type verify_request = {code: string} [@@deriving yojson {strict= false}]

type resync_request = {code1: string; code2: string}
[@@deriving yojson {strict= false}]

type success_response = {success: bool; message: string option [@default None]}
[@@deriving yojson {strict= false}]

let list_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      let%lwt keys = Security_key.get_keys_for_user ~did ctx.db in
      let key_list =
        List.map
          (fun (sk : Security_key.Types.security_key) ->
            ( { id= sk.id
              ; name= sk.name
              ; created_at= sk.created_at
              ; last_used_at= sk.last_used_at
              ; verified= Option.is_some sk.verified_at }
              : security_key_display ) )
          keys
      in
      Dream.json @@ Yojson.Safe.to_string
      @@ list_response_to_yojson {security_keys= key_list} )

let setup_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      let%lwt {name; _} = Xrpc.parse_body ctx.req setup_request_of_yojson in
      let%lwt count = Security_key.count_security_keys ~did ctx.db in
      if count >= Security_key.max_security_keys_per_user then
        Errors.invalid_request
          ( "Maximum "
          ^ string_of_int Security_key.max_security_keys_per_user
          ^ " security keys allowed per account" )
      else
        let name = Option.value name ~default:"Security Key" in
        let%lwt id, secret, uri =
          Security_key.setup_security_key ~did ~name ctx.db
        in
        Dream.json @@ Yojson.Safe.to_string
        @@ setup_response_to_yojson {id; secret; uri} )

let verify_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      match Dream.param ctx.req "id" |> int_of_string_opt with
      | None ->
          Errors.invalid_request "Invalid security key ID"
      | Some id -> (
          let%lwt {code; _} =
            Xrpc.parse_body ctx.req verify_request_of_yojson
          in
          match%lwt Security_key.verify_setup ~id ~did ~code ctx.db with
          | Ok () ->
              Dream.json @@ Yojson.Safe.to_string
              @@ success_response_to_yojson
                   {success= true; message= Some "Security key verified"}
          | Error msg ->
              Errors.invalid_request msg ) )

let resync_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      match Dream.param ctx.req "id" |> int_of_string_opt with
      | None ->
          Errors.invalid_request "Invalid security key ID"
      | Some id -> (
          let%lwt {code1; code2; _} =
            Xrpc.parse_body ctx.req resync_request_of_yojson
          in
          match%lwt Security_key.resync_key ~id ~did ~code1 ~code2 ctx.db with
          | Ok () ->
              Dream.json @@ Yojson.Safe.to_string
              @@ success_response_to_yojson
                   {success= true; message= Some "Security key resynchronized"}
          | Error msg ->
              Errors.invalid_request msg ) )

let delete_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      match Dream.param ctx.req "id" |> int_of_string_opt with
      | None ->
          Errors.invalid_request "Invalid security key ID"
      | Some id ->
          let%lwt _ = Security_key.delete_key ~id ~did ctx.db in
          Dream.json @@ Yojson.Safe.to_string
          @@ success_response_to_yojson
               {success= true; message= Some "Security key removed"} )
