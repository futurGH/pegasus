let get_handler =
  Xrpc.handler (fun ctx ->
      let redirect_url =
        if List.length @@ Dream.all_queries ctx.req > 0 then
          Uri.make ~path:"/oauth/authorize" ~query:(Util.copy_query ctx.req) ()
          |> Uri.to_string
        else "/account"
      in
      let csrf_token = Dream.csrf_token ctx.req in
      let html =
        JSX.render (Templates.Login.make ~redirect_url ~csrf_token ())
      in
      Dream.html html )

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
              let html =
                JSX.render
                  (Templates.Login.make ~redirect_url
                     ~error:"Invalid username or password. Please try again."
                     ~csrf_token () )
              in
              Dream.html ~status:`Unauthorized html
          | Some {did; _} ->
              let%lwt () = Dream.invalidate_session ctx.req in
              let%lwt () = Dream.set_session_field ctx.req "did" did in
              Dream.redirect ctx.req redirect_url )
      | _ ->
          let html =
            JSX.render
              (Templates.Login.make ~redirect_url:"/account"
                 ~error:"Invalid credentials provided. Please try again."
                 ~csrf_token () )
          in
          Dream.html ~status:`Unauthorized html )
