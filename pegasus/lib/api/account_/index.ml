let has_valid_delete_code (actor : Data_store.Types.actor) =
  match (actor.auth_code, actor.auth_code_expires_at) with
  | Some code, Some expires_at ->
      String.starts_with ~prefix:"del-" code && expires_at > Util.now_ms ()
  | _ ->
      false

let parse_email_change_code code =
  if String.starts_with ~prefix:"eml-" code then
    let rest =
      String.sub code 4 (String.length code - 4)
      |> Base64.decode_exn ~alphabet:Base64.uri_safe_alphabet ~pad:false
    in
    match String.split_on_char ':' rest with
    | [token; new_email] ->
        Some (token, new_email)
    | _ ->
        None
  else None

let validate_actor_email_code (actor : Data_store.Types.actor) =
  match (actor.auth_code, actor.auth_code_expires_at) with
  | Some code, Some expires_at when expires_at > Util.now_ms () ->
      parse_email_change_code code
  | _ ->
      None

let get_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Dream.redirect ctx.req "/account/login"
      | Some did -> (
          let%lwt logged_in_users =
            Session.list_logged_in_actors ctx.req ctx.db
          in
          match%lwt Data_store.get_actor_by_identifier did ctx.db with
          | None ->
              Dream.redirect ctx.req "/account/login"
          | Some actor ->
              let current_user : Frontend.AccountSwitcher.actor =
                {did= actor.did; handle= actor.handle; avatar_data_uri= None}
              in
              let csrf_token = Dream.csrf_token ctx.req in
              let deactivated = actor.deactivated_at <> None in
              let email_change_info = validate_actor_email_code actor in
              let email_change_pending = Option.is_some email_change_info in
              let pending_email = Option.map snd email_change_info in
              let delete_pending = has_valid_delete_code actor in
              Util.render_html ~title:"Account"
                (module Frontend.AccountPage)
                ~props:
                  { current_user
                  ; logged_in_users
                  ; csrf_token
                  ; handle= actor.handle
                  ; email= actor.email
                  ; deactivated
                  ; email_change_pending
                  ; pending_email
                  ; email_error= None
                  ; delete_pending
                  ; error= None
                  ; success= None
                  ; delete_error= None } ) )

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Dream.redirect ctx.req "/account/login"
      | Some did -> (
          let%lwt logged_in_users =
            Session.list_logged_in_actors ctx.req ctx.db
          in
          match%lwt Data_store.get_actor_by_identifier did ctx.db with
          | None ->
              Dream.redirect ctx.req "/account/login"
          | Some actor -> (
              let current_user : Frontend.AccountSwitcher.actor =
                {did= actor.did; handle= actor.handle; avatar_data_uri= None}
              in
              let csrf_token = Dream.csrf_token ctx.req in
              let render_page ?error ?success ?email_error ?delete_error () =
                let%lwt actor_opt =
                  Data_store.get_actor_by_identifier did ctx.db
                in
                let actor = Option.get actor_opt in
                let deactivated = actor.deactivated_at <> None in
                let email_change_info = validate_actor_email_code actor in
                let email_change_pending = Option.is_some email_change_info in
                let pending_email = Option.map snd email_change_info in
                let delete_pending = has_valid_delete_code actor in
                Util.render_html ~title:"Account"
                  (module Frontend.AccountPage)
                  ~props:
                    { current_user= {current_user with handle= actor.handle}
                    ; logged_in_users
                    ; csrf_token
                    ; handle= actor.handle
                    ; email= actor.email
                    ; deactivated
                    ; email_change_pending
                    ; pending_email
                    ; email_error
                    ; delete_pending
                    ; error
                    ; success
                    ; delete_error }
              in
              match%lwt Dream.form ctx.req with
              | `Ok fields -> (
                  let action = List.assoc_opt "action" fields in
                  match action with
                  | Some "save" -> (
                      let new_handle =
                        List.assoc_opt "handle" fields
                        |> Option.value ~default:actor.handle
                      in
                      let new_password = List.assoc_opt "password" fields in
                      (* update handle if changed *)
                      let%lwt handle_result =
                        if new_handle <> actor.handle then
                          match Util.validate_handle new_handle with
                          | Error e ->
                              Lwt.return_error e
                          | Ok () -> (
                            match%lwt
                              Data_store.get_actor_by_identifier new_handle
                                ctx.db
                            with
                            | Some _ ->
                                Lwt.return_error "Handle already in use"
                            | None ->
                                let%lwt () =
                                  Data_store.update_actor_handle ~did
                                    ~handle:new_handle ctx.db
                                in
                                Lwt.return_ok () )
                        else Lwt.return_ok ()
                      in
                      match handle_result with
                      | Error e ->
                          render_page ~error:e ()
                      | Ok () ->
                          (* update password if provided *)
                          let%lwt () =
                            match new_password with
                            | Some pw when String.length pw > 0 ->
                                Data_store.update_password ~did ~password:pw
                                  ctx.db
                            | _ ->
                                Lwt.return_unit
                          in
                          render_page ~success:"Changes saved." () )
                  | Some "reactivate" ->
                      let%lwt () = Data_store.activate_actor did ctx.db in
                      let%lwt _ =
                        Sequencer.sequence_account ctx.db ~did ~active:true
                          ~status:`Active ()
                      in
                      render_page ~success:"Your account has been reactivated."
                        ()
                  | Some "deactivate" ->
                      let%lwt () = Data_store.deactivate_actor did ctx.db in
                      let%lwt _ =
                        Sequencer.sequence_account ctx.db ~did ~active:false
                          ~status:`Deactivated ()
                      in
                      let%lwt () = Session.Raw.clear_session ctx.req in
                      Dream.redirect ctx.req "/account/login"
                  | Some "request_delete" ->
                      let code = "del-" ^ Mist.Tid.now () in
                      let expires_at = Util.now_ms () + (15 * 60 * 1000) in
                      let%lwt () =
                        Data_store.set_auth_code ~did ~code ~expires_at ctx.db
                      in
                      let%lwt () =
                        Util.send_email_or_log ~recipients:[To actor.email]
                          ~subject:"Account deletion confirmation"
                          ~body:
                            (Plain
                               (Printf.sprintf
                                  "Confirm that you would like to delete the \
                                   account %s (%s) by entering the following \
                                   code: %s"
                                  actor.handle did code ) )
                      in
                      render_page ()
                  | Some "confirm_delete" -> (
                      let token =
                        List.assoc_opt "token" fields
                        |> Option.value ~default:""
                      in
                      match (actor.auth_code, actor.auth_code_expires_at) with
                      | Some code, Some expires_at
                        when code = token && expires_at > Util.now_ms () ->
                          let%lwt () = Data_store.delete_actor did ctx.db in
                          let%lwt _ =
                            Sequencer.sequence_account ctx.db ~did ~active:false
                              ~status:`Deleted ()
                          in
                          let%lwt () = Session.Raw.clear_session ctx.req in
                          Dream.redirect ctx.req "/account/login"
                      | _ ->
                          render_page
                            ~delete_error:
                              "Invalid or expired confirmation code."
                            () )
                  | Some "cancel_delete" ->
                      let%lwt () = Data_store.clear_auth_code ~did ctx.db in
                      render_page ()
                  | Some "request_email_change" -> (
                      let new_email =
                        List.assoc_opt "new_email" fields
                        |> Option.value ~default:"" |> String.trim
                      in
                      if String.length new_email = 0 then
                        render_page
                          ~email_error:"Please enter a new email address." ()
                      else if new_email = actor.email then
                        render_page
                          ~email_error:"That's already your email address." ()
                      else
                        match%lwt
                          Data_store.get_actor_by_identifier new_email ctx.db
                        with
                        | Some _ ->
                            render_page ~email_error:"Email is already in use."
                              ()
                        | None ->
                            let token = Mist.Tid.now () in
                            let code = token ^ ":" ^ new_email in
                            let code =
                              Base64.encode_exn
                                ~alphabet:Base64.uri_safe_alphabet ~pad:false
                                code
                            in
                            let code = "eml-" ^ code in
                            let expires_at =
                              Util.now_ms () + (15 * 60 * 1000)
                            in
                            let%lwt () =
                              Data_store.set_auth_code ~did ~code ~expires_at
                                ctx.db
                            in
                            let%lwt () =
                              Util.send_email_or_log
                                ~recipients:[To actor.email]
                                ~subject:"Email change confirmation"
                                ~body:
                                  (Plain
                                     (Printf.sprintf
                                        "Confirm that you would like to update \
                                         the email address for @%s (%s) from \
                                         %s to %s by entering the following \
                                         code: %s"
                                        actor.handle did actor.email new_email
                                        code ) )
                            in
                            render_page () )
                  | Some "confirm_email_change" -> (
                      let token =
                        List.assoc_opt "token" fields
                        |> Option.value ~default:"" |> String.trim
                      in
                      match validate_actor_email_code actor with
                      | Some (_, new_email) when Some token = actor.auth_code ->
                          let%lwt () =
                            Data_store.update_email ~did ~email:new_email ctx.db
                          in
                          let%lwt () = Data_store.clear_auth_code ~did ctx.db in
                          render_page ~success:"Email address updated." ()
                      | _ ->
                          render_page
                            ~email_error:"Invalid or expired verification code."
                            () )
                  | Some "cancel_email_change" ->
                      let%lwt () = Data_store.clear_auth_code ~did ctx.db in
                      render_page ()
                  | _ ->
                      render_page ~error:"Invalid action." () )
              | _ ->
                  render_page ~error:"Invalid form submission." () ) ) )
