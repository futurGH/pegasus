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
              let%lwt () = Dream.invalidate_session ctx.req in
              let%lwt () = Session.log_in_did ctx.req did in
              Dream.redirect ctx.req redirect_url )
      | _ ->
          let redirect_url = "/account" in
          let error = "Something went wrong, go back and try again." in
          Util.render_html ~status:`Unauthorized ~title:"Login"
            (module Frontend.LoginPage)
            ~props:{redirect_url; csrf_token; error= Some error} )
