let has_valid_delete_code (actor : Data_store.Types.actor) =
  match (actor.auth_code, actor.auth_code_expires_at) with
  | Some code, Some expires_at ->
      String.starts_with ~prefix:"del-" code && expires_at > Util.Time.now_ms ()
  | _ ->
      false

let has_valid_email_change_code (actor : Data_store.Types.actor) =
  match (actor.auth_code, actor.auth_code_expires_at, actor.pending_email) with
  | Some _, Some expires_at, Some _ ->
      expires_at > Util.Time.now_ms ()
  | _ ->
      false

let has_valid_email_confirmation_code (actor : Data_store.Types.actor) =
  match (actor.auth_code, actor.auth_code_expires_at, actor.pending_email) with
  | Some _, Some expires_at, None ->
      expires_at > Util.Time.now_ms ()
  | _ ->
      false

let get_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Dream.redirect ctx.req "/account/login"
      | Some did -> (
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
              let deactivated = actor.deactivated_at <> None in
              let email_confirmed = actor.email_confirmed_at <> None in
              let email_confirmation_pending =
                has_valid_email_confirmation_code actor
              in
              let email_change_pending = has_valid_email_change_code actor in
              let pending_email = actor.pending_email in
              let delete_pending = has_valid_delete_code actor in
              Util.Html.render_page ~title:"Account"
                (module Frontend.AccountPage)
                ~props:
                  { current_user
                  ; logged_in_users
                  ; csrf_token
                  ; handle= actor.handle
                  ; email= actor.email
                  ; deactivated
                  ; email_confirmed
                  ; email_confirmation_pending
                  ; email_confirmation_error= None
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
          let%lwt current_user, logged_in_users =
            Session.list_logged_in_actors ctx.req ctx.db
          in
          match%lwt Data_store.get_actor_by_identifier did ctx.db with
          | None ->
              Dream.redirect ctx.req "/account/login"
          | Some actor -> (
              let current_user =
                Option.value
                  ~default:
                    {did= actor.did; handle= actor.handle; avatar_data_uri= None}
                  current_user
              in
              let csrf_token = Dream.csrf_token ctx.req in
              let render_page ?error ?success ?email_error
                  ?email_confirmation_error ?delete_error () =
                let%lwt actor_opt =
                  Data_store.get_actor_by_identifier did ctx.db
                in
                let actor = Option.get actor_opt in
                let deactivated = actor.deactivated_at <> None in
                let email_confirmed = actor.email_confirmed_at <> None in
                let email_confirmation_pending =
                  has_valid_email_confirmation_code actor
                in
                let email_change_pending = has_valid_email_change_code actor in
                let pending_email = actor.pending_email in
                let delete_pending = has_valid_delete_code actor in
                Util.Html.render_page ~title:"Account"
                  (module Frontend.AccountPage)
                  ~props:
                    { current_user= {current_user with handle= actor.handle}
                    ; logged_in_users
                    ; csrf_token
                    ; handle= actor.handle
                    ; email= actor.email
                    ; deactivated
                    ; email_confirmed
                    ; email_confirmation_pending
                    ; email_confirmation_error
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
                      (* update handle if changed *)
                      let%lwt handle_result =
                        if new_handle <> actor.handle then
                          Identity_util.update_handle ~did
                            ~handle:new_handle ctx.db
                        else Lwt.return_ok ()
                      in
                      match handle_result with
                      | Error (InvalidFormat e)
                      | Error (TooLong e)
                      | Error (TooShort e) ->
                          render_page ~error:("Handle " ^ e) ()
                      | Error HandleTaken ->
                          render_page ~error:"Handle already taken" ()
                      | Error (InternalServerError _) ->
                          render_page ~error:"Internal server error" ()
                      | Ok () ->
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
                      let%lwt _ =
                        Server.DeactivateAccount.deactivate_account ~did ctx.db
                      in
                      let%lwt () = Session.Raw.clear_session ctx.req in
                      Dream.redirect ctx.req "/account/login"
                  | Some "request_delete" ->
                      let%lwt () =
                        Server.RequestAccountDelete.request_account_delete actor
                          ctx.db
                      in
                      render_page ()
                  | Some "confirm_delete" -> (
                      let token =
                        List.assoc_opt "token" fields
                        |> Option.value ~default:""
                      in
                      match (actor.auth_code, actor.auth_code_expires_at) with
                      | Some code, Some expires_at
                        when String.starts_with ~prefix:"del-" code
                             && code = token
                             && expires_at > Util.Time.now_ms () ->
                          let%lwt _ =
                            Server.DeleteAccount.delete_account ~did ctx.db
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
                            let%lwt _token_required =
                              Server.RequestEmailUpdate.request_email_update
                                ~pending_email:new_email actor ctx.db
                            in
                            render_page () )
                  | Some "confirm_email_change" -> (
                      let token =
                        List.assoc_opt "token" fields |> Option.map String.trim
                      in
                      match%lwt
                        match (actor.auth_code, actor.auth_code_expires_at) with
                        | Some code, Some expiry
                          when Some code = token && expiry > Util.Time.now_ms () ->
                            Server.UpdateEmail.update_email ~token actor ctx.db
                        | _ ->
                            Lwt.return_error Server.UpdateEmail.InvalidToken
                      with
                      | Ok _ ->
                          render_page ~success:"Email address updated." ()
                      | Error Server.UpdateEmail.ExpiredToken
                      | Error Server.UpdateEmail.InvalidToken
                      | Error Server.UpdateEmail.NoEmailProvided ->
                          render_page
                            ~email_error:"Invalid or expired verification code."
                            ()
                      | Error Server.UpdateEmail.TokenRequired ->
                          render_page ~email_error:"Verification code required."
                            () )
                  | Some "cancel_email_change" ->
                      let%lwt () = Data_store.clear_auth_code ~did ctx.db in
                      render_page ()
                  | Some "request_email_confirmation" -> (
                    match%lwt
                      Server.RequestEmailConfirmation.request_email_confirmation
                        actor ctx.db
                    with
                    | Ok () ->
                        render_page ()
                    | Error Server.RequestEmailConfirmation.AlreadyConfirmed ->
                        render_page
                          ~email_confirmation_error:
                            "Email is already confirmed."
                          () )
                  | Some "confirm_email_confirmation" -> (
                      let token =
                        List.assoc_opt "token" fields
                        |> Option.value ~default:"" |> String.trim
                      in
                      match%lwt
                        Server.ConfirmEmail.confirm_email ~email:actor.email
                          ~token actor ctx.db
                      with
                      | Ok () ->
                          render_page ~success:"Email confirmed." ()
                      | Error Server.ConfirmEmail.ExpiredToken
                      | Error Server.ConfirmEmail.InvalidToken ->
                          render_page
                            ~email_confirmation_error:
                              "Invalid or expired confirmation code."
                            ()
                      | Error Server.ConfirmEmail.EmailMismatch ->
                          render_page
                            ~email_confirmation_error:"Email mismatch." () )
                  | Some "cancel_email_confirmation" ->
                      let%lwt () = Data_store.clear_auth_code ~did ctx.db in
                      render_page ()
                  | _ ->
                      render_page ~error:"Invalid action." () )
              | _ ->
                  render_page ~error:"Invalid form submission." () ) ) )
