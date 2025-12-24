let get_handler =
  Xrpc.handler (fun ctx ->
      let redirect_url =
        if List.length @@ Dream.all_queries ctx.req > 0 then
          Uri.make ~path:"/oauth/authorize" ~query:(Util.copy_query ctx.req) ()
          |> Uri.to_string
        else "/account"
      in
      let csrf_token = Dream.csrf_token ctx.req in
      Util.render_html ~title:"Login"
        (module Frontend.LoginPage)
        ~props:{redirect_url; csrf_token; error= None} )

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
          let identifier = List.assoc "identifier" fields in
          let password = List.assoc "password" fields in
          let redirect_url =
            List.assoc_opt "redirect_url" fields
            |> Option.value ~default:"/account"
          in
          let%lwt actor =
            Data_store.try_login ~id:identifier ~password ctx.db
          in
          match actor with
          | None ->
              let error = "Invalid username or password. Please try again." in
              Util.render_html ~status:`Unauthorized ~title:"Login"
                (module Frontend.LoginPage)
                ~props:{redirect_url; csrf_token; error= Some error}
          | Some {did; _} ->
              let%lwt () = Session.log_in_did ctx.req did in
              Dream.redirect ctx.req redirect_url )
      | _ ->
          let redirect_url = "/account" in
          let error = "Something went wrong, go back and try again." in
          Util.render_html ~status:`Unauthorized ~title:"Login"
            (module Frontend.LoginPage)
            ~props:{redirect_url; csrf_token; error= Some error} )
