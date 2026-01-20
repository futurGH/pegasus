type passkey_info =
  { id: int
  ; name: string
  ; created_at: int
  ; last_used_at: int option [@default None] }
[@@deriving yojson {strict= false}]

type success_response = {success: bool; message: string option [@default None]}
[@@deriving yojson {strict= false}]

type error_response = {error: string} [@@deriving yojson {strict= false}]

let get_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      let%lwt current_user, logged_in_users =
        Session.list_logged_in_actors ctx.req ctx.db
      in
      match%lwt Data_store.get_actor_by_identifier did ctx.db with
      | None ->
          Dream.redirect ctx.req "/account/login"
      | Some actor ->
          let current_user =
            Option.value
              ~default:
                {did= actor.did; handle= actor.handle; avatar_data_uri= None}
              current_user
          in
          let csrf_token = Dream.csrf_token ctx.req in
          let%lwt passkeys = Passkey.get_credentials_for_user ~did ctx.db in
          let passkey_list =
            List.map
              (fun (pk : Passkey.Types.passkey) ->
                ( { id= pk.id
                  ; name= pk.name
                  ; created_at= pk.created_at
                  ; last_used_at= pk.last_used_at }
                  : Frontend.AccountSecurityPage.passkey_display ) )
              passkeys
          in
          let%lwt security_keys = Security_key.get_keys_for_user ~did ctx.db in
          let security_key_list =
            List.map
              (fun (sk : Security_key.Types.security_key) ->
                ( { id= sk.id
                  ; name= sk.name
                  ; created_at= sk.created_at
                  ; last_used_at= sk.last_used_at
                  ; verified= Option.is_some sk.verified_at }
                  : Frontend.AccountSecurityPage.security_key_display ) )
              security_keys
          in
          let%lwt two_fa_status = Two_factor.get_status ~did ctx.db in
          let error = Dream.query ctx.req "error" in
          let success = Dream.query ctx.req "success" in
          Util.Html.render_page ~title:"Security"
            (module Frontend.AccountSecurityPage)
            ~props:
              { current_user
              ; logged_in_users
              ; csrf_token
              ; passkeys= passkey_list
              ; security_keys= security_key_list
              ; totp_enabled= two_fa_status.totp_enabled
              ; email_2fa_enabled= two_fa_status.email_2fa_enabled
              ; backup_codes_remaining= two_fa_status.backup_codes_remaining
              ; error
              ; success } )

let post_handler =
  Xrpc.handler (fun ctx ->
      let%lwt did = Session.get_current_did_exn ctx.req in
      match%lwt Dream.form ctx.req with
      | `Ok fields -> (
          let action = List.assoc_opt "action" fields in
          match action with
          | Some "change_password" -> (
              let current_password = List.assoc_opt "current_password" fields in
              let new_password = List.assoc_opt "new_password" fields in
              let confirm_password = List.assoc_opt "confirm_password" fields in
              match (current_password, new_password, confirm_password) with
              | Some current, Some new_pw, Some confirm when new_pw = confirm
                -> (
                match%lwt Data_store.get_actor_by_identifier did ctx.db with
                | None ->
                    Dream.redirect ctx.req
                      "/account/security?error=User+not+found."
                | Some actor ->
                    let hash = Bcrypt.hash_of_string actor.password_hash in
                    if Bcrypt.verify current hash then
                      let%lwt () =
                        Data_store.update_password ~did ~password:new_pw ctx.db
                      in
                      Dream.redirect ctx.req
                        "/account/security?success=Password+updated%21"
                    else
                      Dream.redirect ctx.req
                        "/account/security?error=Incorrect+current+password." )
              | Some _, Some _, Some _ ->
                  Dream.redirect ctx.req
                    "/account/security?error=Passwords+do+not+match."
              | _ ->
                  Dream.redirect ctx.req
                    "/account/security?error=Missing+required+fields." )
          | Some "enable_email_2fa" ->
              let%lwt () = Two_factor.enable_email_2fa ~did ctx.db in
              Dream.redirect ctx.req
                "/account/security?success=Email+2FA+enabled%21"
          | Some "disable_email_2fa" ->
              let%lwt () = Two_factor.disable_email_2fa ~did ctx.db in
              Dream.redirect ctx.req
                "/account/security?success=Email+2FA+disabled."
          | _ ->
              Dream.redirect ctx.req "/account/security" )
      | _ ->
          Dream.redirect ctx.req "/account/security" )
