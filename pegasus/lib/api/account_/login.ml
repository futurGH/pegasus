let get_handler =
  Xrpc.handler (fun ctx ->
      let redirect_url =
        Dream.query ctx.req "redirect_url" |> Option.value ~default:"/"
      in
      let html = JSX.render (Templates.Login.make ~redirect_url ()) in
      Dream.html html )

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Dream.form ctx.req with
      | `Ok fields -> (
          let identifier = List.assoc "identifier" fields in
          let password = List.assoc "password" fields in
          let redirect_url =
            List.assoc_opt "redirect_url" fields |> Option.value ~default:"/"
          in
          let%lwt actor =
            Data_store.try_login ~id:identifier ~password ctx.db
          in
          match actor with
          | None ->
              let html =
                JSX.render
                  (Templates.Login.make ~redirect_url
                     ~error:"Invalid username or password. Please try again." () )
              in
              Dream.html ~status:`Unauthorized html
          | Some {did; _} ->
              let%lwt () = Dream.invalidate_session ctx.req in
              let%lwt () = Dream.set_session_field ctx.req "did" did in
              Dream.redirect ctx.req redirect_url )
      | _ ->
          let html =
            JSX.render
              (Templates.Login.make ~redirect_url:"/"
                 ~error:"Invalid credentials provided. Please try again." () )
          in
          Dream.html ~status:`Unauthorized html )
