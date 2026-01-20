let get_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let step =
        Dream.query ctx.req "step" |> Option.value ~default:"request"
      in
      Util.Html.render_page ~title:"Reset Password"
        (module Frontend.PasswordResetPage)
        ~props:{csrf_token; step; email_sent_to= None; error= None} )

let post_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      match%lwt Dream.form ctx.req with
      | `Ok fields -> (
          let step =
            List.assoc_opt "step" fields |> Option.value ~default:"request"
          in
          match step with
          | "reset" -> (
              let token =
                List.assoc_opt "token" fields |> Option.value ~default:""
              in
              let password =
                List.assoc_opt "password" fields |> Option.value ~default:""
              in
              if String.length token = 0 then
                Util.Html.render_page ~status:`Bad_Request ~title:"Reset Password"
                  (module Frontend.PasswordResetPage)
                  ~props:
                    { csrf_token
                    ; step= "reset"
                    ; email_sent_to= None
                    ; error= Some "Please enter the reset code." }
              else if String.length password < 8 then
                Util.Html.render_page ~status:`Bad_Request ~title:"Reset Password"
                  (module Frontend.PasswordResetPage)
                  ~props:
                    { csrf_token
                    ; step= "reset"
                    ; email_sent_to= None
                    ; error= Some "Password must be at least 8 characters." }
              else
                match%lwt
                  Server.ResetPassword.reset_password ~token ~password ctx.db
                with
                | Ok _ ->
                    Util.Html.render_page ~title:"Reset Password"
                      (module Frontend.PasswordResetPage)
                      ~props:
                        { csrf_token
                        ; step= "success"
                        ; email_sent_to= None
                        ; error= None }
                | Error Server.ResetPassword.InvalidToken
                | Error Server.ResetPassword.ExpiredToken ->
                    Util.Html.render_page ~status:`Bad_Request ~title:"Reset Password"
                      (module Frontend.PasswordResetPage)
                      ~props:
                        { csrf_token
                        ; step= "reset"
                        ; email_sent_to= None
                        ; error=
                            Some
                              "Invalid or expired reset code. Please request a \
                               new one." } )
          | _ ->
              let email =
                List.assoc_opt "email" fields
                |> Option.value ~default:""
                |> String.lowercase_ascii
              in
              if String.length email = 0 then
                Util.Html.render_page ~status:`Bad_Request ~title:"Reset Password"
                  (module Frontend.PasswordResetPage)
                  ~props:
                    { csrf_token
                    ; step= "request"
                    ; email_sent_to= None
                    ; error= Some "Please enter your email address." }
              else
                let%lwt () =
                  match%lwt Data_store.get_actor_by_identifier email ctx.db with
                  | Some actor ->
                      Server.RequestPasswordReset.request_password_reset actor
                        ctx.db
                  | None ->
                      Lwt.return_unit
                in
                Util.Html.render_page ~title:"Reset Password"
                  (module Frontend.PasswordResetPage)
                  ~props:
                    { csrf_token
                    ; step= "reset"
                    ; email_sent_to= Some email
                    ; error= None } )
      | _ ->
          Util.Html.render_page ~status:`Bad_Request ~title:"Reset Password"
            (module Frontend.PasswordResetPage)
            ~props:
              { csrf_token
              ; step= "request"
              ; email_sent_to= None
              ; error= Some "Invalid form submission. Please try again." } )
