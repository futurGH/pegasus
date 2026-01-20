let get_handler =
  Xrpc.handler (fun ctx ->
      let redirect_url =
        if List.length @@ Dream.all_queries ctx.req > 0 then
          Uri.make ~path:"/oauth/authorize" ~query:(Util.Http.copy_query ctx.req) ()
          |> Uri.to_string
        else "/account"
      in
      let csrf_token = Dream.csrf_token ctx.req in
      Util.Html.render_page ~title:"Login"
        (module Frontend.LoginPage)
        ~props:
          { redirect_url
          ; csrf_token
          ; error= None
          ; two_fa_required= false
          ; pending_2fa_token= None
          ; two_fa_methods= None } )

type switch_account_response =
  {success: bool; error: string option [@default None]}
[@@deriving yojson {strict= false}]

let switch_account_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Dream.form ~csrf:false ctx.req with
      | `Ok fields -> (
          let did = List.assoc_opt "did" fields in
          match did with
          | Some did ->
              let%lwt logged_in_dids = Session.Raw.get_logged_in_dids ctx.req in
              if List.mem did logged_in_dids then
                let%lwt () = Session.Raw.set_current_did ctx.req did in
                Dream.json @@ Yojson.Safe.to_string
                @@ switch_account_response_to_yojson {success= true; error= None}
              else
                Dream.json ~status:`Bad_Request
                @@ Yojson.Safe.to_string
                @@ switch_account_response_to_yojson
                     { success= false
                     ; error= Some "not logged in as this account" }
          | None ->
              Dream.json ~status:`Bad_Request
              @@ Yojson.Safe.to_string
              @@ switch_account_response_to_yojson
                   {success= false; error= Some "missing did parameter"} )
      | _ ->
          Dream.json ~status:`Bad_Request
          @@ Yojson.Safe.to_string
          @@ switch_account_response_to_yojson
               {success= false; error= Some "invalid form submission"} )

let post_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      match%lwt Dream.form ctx.req with
      | `Ok fields -> (
          let redirect_url =
            List.assoc_opt "redirect_url" fields
            |> Option.value ~default:"/account"
          in
          (* check if this is a 2FA verification step *)
          let pending_token = List.assoc_opt "pending_2fa_token" fields in
          let two_fa_code = List.assoc_opt "two_fa_code" fields in
          match (pending_token, two_fa_code) with
          | Some token, Some code -> (
            match%lwt
              Two_factor.get_pending_session ~session_token:token ctx.db
            with
            | None ->
                let error = "Session expired. Please try again." in
                Util.Html.render_page ~status:`Unauthorized ~title:"Login"
                  (module Frontend.LoginPage)
                  ~props:
                    { redirect_url
                    ; csrf_token
                    ; error= Some error
                    ; two_fa_required= false
                    ; pending_2fa_token= None
                    ; two_fa_methods= None }
            | Some pending -> (
                let%lwt result =
                  Two_factor.verify_code_with_pending_session ~pending ~code
                    ctx.db
                in
                match result with
                | Ok did ->
                    let%lwt () =
                      Two_factor.delete_pending_session ~session_token:token
                        ctx.db
                    in
                    let%lwt () = Session.log_in_did ctx.req did in
                    Dream.redirect ctx.req redirect_url
                | Error _ ->
                    let%lwt methods =
                      Two_factor.get_available_methods ~did:pending.did ctx.db
                    in
                    Util.Html.render_page ~status:`Unauthorized ~title:"Login"
                      (module Frontend.LoginPage)
                      ~props:
                        { redirect_url
                        ; csrf_token
                        ; error= Some "Invalid verification code"
                        ; two_fa_required= true
                        ; pending_2fa_token= Some token
                        ; two_fa_methods= Some methods } ) )
          | _ -> (
              let identifier = List.assoc "identifier" fields in
              let password = List.assoc "password" fields in
              let%lwt actor =
                Data_store.try_login ~id:identifier ~password ctx.db
              in
              match actor with
              | None ->
                  let error =
                    "Invalid username or password. Please try again."
                  in
                  Util.Html.render_page ~status:`Unauthorized ~title:"Login"
                    (module Frontend.LoginPage)
                    ~props:
                      { redirect_url
                      ; csrf_token
                      ; error= Some error
                      ; two_fa_required= false
                      ; pending_2fa_token= None
                      ; two_fa_methods= None }
              | Some actor ->
                  let%lwt is_2fa_enabled =
                    Two_factor.is_2fa_enabled ~did:actor.did ctx.db
                  in
                  if is_2fa_enabled then
                    let%lwt session_token =
                      Two_factor.create_pending_session ~did:actor.did ctx.db
                    in
                    let%lwt methods =
                      Two_factor.get_available_methods ~did:actor.did ctx.db
                    in
                    (* if email-only 2FA, send email code now *)
                    let%lwt () =
                      if methods.email && not methods.totp then
                        let%lwt () =
                          Two_factor.send_email_code ~session_token ~actor
                            ctx.db
                        in
                        Lwt.return ()
                      else Lwt.return ()
                    in
                    Util.Html.render_page ~title:"Login"
                      (module Frontend.LoginPage)
                      ~props:
                        { redirect_url
                        ; csrf_token
                        ; error= None
                        ; two_fa_required= true
                        ; pending_2fa_token= Some session_token
                        ; two_fa_methods= Some methods }
                  else
                    let%lwt () = Session.log_in_did ctx.req actor.did in
                    Dream.redirect ctx.req redirect_url ) )
      | _ ->
          let redirect_url = "/account" in
          let error = "Something went wrong, go back and try again." in
          Util.Html.render_page ~status:`Unauthorized ~title:"Login"
            (module Frontend.LoginPage)
            ~props:
              { redirect_url
              ; csrf_token
              ; error= Some error
              ; two_fa_required= false
              ; pending_2fa_token= None
              ; two_fa_methods= None } )
