type check_handle_response =
  {valid: bool; available: bool; error: string option [@default None]}
[@@deriving yojson {strict= false}]

let check_handle_handler =
  Xrpc.handler (fun ctx ->
      let handle_input =
        Dream.query ctx.req "handle" |> Option.value ~default:""
      in
      if String.length handle_input = 0 then
        Dream.json @@ Yojson.Safe.to_string
        @@ check_handle_response_to_yojson
             {valid= false; available= false; error= Some "Handle is required"}
      else
        let handle = handle_input ^ "." ^ Env.hostname in
        let validation_result = Util.validate_handle handle in
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
      Util.render_html ~title:"Sign Up"
        (module Frontend.SignupPage)
        ~props:{csrf_token; invite_required; hostname; error= None} )

let post_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      let invite_required = Env.invite_required in
      let hostname = Env.hostname in
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
          let handle = handle_input ^ "." ^ Env.hostname in
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
              Util.render_html ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { csrf_token
                  ; invite_required
                  ; hostname
                  ; error= Some "An invite code is required to sign up." }
          | Error Server.CreateAccount.InvalidInviteCode ->
              Util.render_html ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { csrf_token
                  ; invite_required
                  ; hostname
                  ; error= Some "Invalid invite code." }
          | Error (Server.CreateAccount.InvalidHandle e) ->
              Util.render_html ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:{csrf_token; invite_required; hostname; error= Some e}
          | Error Server.CreateAccount.EmailAlreadyExists ->
              Util.render_html ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { csrf_token
                  ; invite_required
                  ; hostname
                  ; error= Some "An account with that email already exists." }
          | Error Server.CreateAccount.HandleAlreadyExists ->
              Util.render_html ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { csrf_token
                  ; invite_required
                  ; hostname
                  ; error= Some "An account with that handle already exists." }
          | Error Server.CreateAccount.DidAlreadyExists ->
              Util.render_html ~status:`Bad_Request ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { csrf_token
                  ; invite_required
                  ; hostname
                  ; error= Some "An account with that DID already exists." }
          | Error (Server.CreateAccount.PlcError _) ->
              Util.render_html ~status:`Internal_Server_Error ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { csrf_token
                  ; invite_required
                  ; hostname
                  ; error=
                      Some
                        "Failed to create your identity. Please try again \
                         later." }
          | Error Server.CreateAccount.InviteUseFailure ->
              Util.render_html ~status:`Internal_Server_Error ~title:"Sign Up"
                (module Frontend.SignupPage)
                ~props:
                  { csrf_token
                  ; invite_required
                  ; hostname
                  ; error=
                      Some "Failed to use your invite code. Please try again."
                  }
          | Ok {did; _} ->
              let%lwt () = Session.log_in_did ctx.req did in
              Dream.redirect ctx.req "/account" )
      | _ ->
          Util.render_html ~status:`Bad_Request ~title:"Sign Up"
            (module Frontend.SignupPage)
            ~props:
              { csrf_token
              ; invite_required
              ; hostname
              ; error= Some "Invalid form submission. Please try again." } )
