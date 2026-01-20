type check_handle_response =
  {valid: bool; available: bool; error: string option [@default None]}
[@@deriving yojson {strict= false}]

let check_handle_handler =
  Xrpc.handler (fun ctx ->
      let handle_input =
        Dream.query ctx.req "handle" |> Option.value ~default:""
      in
      let hostname_suffix = "." ^ Env.hostname in
      if String.length handle_input = 0 then
        Dream.json @@ Yojson.Safe.to_string
        @@ check_handle_response_to_yojson
             {valid= false; available= false; error= Some "Handle is required"}
      else
        let handle =
          if String.contains handle_input '.' then handle_input
          else handle_input ^ hostname_suffix
        in
        let validation_result = Identity_util.validate_handle handle in
        match validation_result with
        | Error (InvalidFormat e) | Error (TooLong e) | Error (TooShort e) ->
            Dream.json @@ Yojson.Safe.to_string
            @@ check_handle_response_to_yojson
                 {valid= false; available= false; error= Some ("Handle " ^ e)}
        | Ok () -> (
            let%lwt existing =
              Data_store.get_actor_by_identifier handle ctx.db
            in
            match existing with
            | Some _ ->
                Dream.json @@ Yojson.Safe.to_string
                @@ check_handle_response_to_yojson
                     { valid= true
                     ; available= false
                     ; error= Some "Handle is already taken" }
            | None ->
                Dream.json @@ Yojson.Safe.to_string
                @@ check_handle_response_to_yojson
                     {valid= true; available= true; error= None} ) )

let get_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let invite_required = Env.invite_required in
      let hostname = Env.hostname in
      Util.Html.render_page ~title:"Sign Up"
        (module Frontend.SignupPage)
        ~props:{csrf_token; invite_required; hostname; error= None} )

let post_handler =
  Xrpc.handler (fun ctx ->
      let props : Frontend.SignupPage.props =
        { csrf_token= Dream.csrf_token ctx.req
        ; invite_required= Env.invite_required
        ; hostname= Env.hostname
        ; error= None }
      in
      match%lwt Dream.form ctx.req with
      | `Ok fields -> (
          let invite_code =
            List.assoc_opt "invite_code" fields
            |> Option.map String.trim
            |> fun c ->
            Option.bind c (fun s ->
                if String.length s = 0 then None else Some s )
          in
          let handle_input =
            List.assoc_opt "handle" fields |> Option.value ~default:""
          in
          let hostname_suffix = "." ^ Env.hostname in
          let handle =
            if String.contains handle_input '.' then handle_input
            else handle_input ^ hostname_suffix
          in
          let email =
            List.assoc_opt "email" fields
            |> Option.value ~default:"" |> String.lowercase_ascii
          in
          let password =
            List.assoc_opt "password" fields |> Option.value ~default:""
          in
          match%lwt
            Server.CreateAccount.create_account ~email ~handle ~password
              ?invite_code ctx.db
          with
          | Error Server.CreateAccount.InviteCodeRequired ->
              Util.Html.render_page ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { props with
                    error= Some "An invite code is required to sign up." }
          | Error Server.CreateAccount.InvalidInviteCode ->
              Util.Html.render_page ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:{props with error= Some "Invalid invite code."}
          | Error (Server.CreateAccount.InvalidHandle e) ->
              Util.Html.render_page ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:{props with error= Some e}
          | Error Server.CreateAccount.EmailAlreadyExists ->
              Util.Html.render_page ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { props with
                    error= Some "An account with that email already exists." }
          | Error Server.CreateAccount.HandleAlreadyExists ->
              Util.Html.render_page ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { props with
                    error= Some "An account with that handle already exists." }
          | Error Server.CreateAccount.DidAlreadyExists ->
              Util.Html.render_page ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { props with
                    error= Some "An account with that DID already exists." }
          | Error (Server.CreateAccount.PlcError _) ->
              Util.Html.render_page ~status:`Internal_Server_Error ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { props with
                    error=
                      Some
                        "Failed to create your identity. Please try again \
                         later." }
          | Error Server.CreateAccount.InviteUseFailure ->
              Util.Html.render_page ~status:`Internal_Server_Error ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { props with
                    error=
                      Some "Failed to use your invite code. Please try again."
                  }
          | Ok {did; _} ->
              let%lwt () = Session.log_in_did ctx.req did in
              Dream.redirect ctx.req "/account" )
      | _ ->
          Util.Html.render_page ~status:`Bad_Request ~title:"Sign Up"
            (module Frontend.SignupPage)
            ~props:
              { props with
                error= Some "Invalid form submission. Please try again." } )
