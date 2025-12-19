let get_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | true ->
          Dream.redirect ctx.req "/admin/users"
      | false ->
          let csrf_token = Dream.csrf_token ctx.req in
          Util.render_html ~title:"Admin Login"
            (module Frontend.AdminLoginPage)
            ~props:{csrf_token; error= None} )

let post_handler =
  Xrpc.handler (fun ctx ->
      let csrf_token = Dream.csrf_token ctx.req in
      match%lwt Dream.form ctx.req with
      | `Ok fields ->
          let password =
            List.assoc_opt "password" fields |> Option.value ~default:""
          in
          if password = Env.admin_password then
            let%lwt () = Session.set_admin_authenticated ctx.req true in
            Dream.redirect ctx.req "/admin/users"
          else
            Util.render_html ~status:`Unauthorized ~title:"Admin Login"
              (module Frontend.AdminLoginPage)
              ~props:{csrf_token; error= Some "Invalid password."}
      | _ ->
          Util.render_html ~status:`Unauthorized ~title:"Admin Login"
            (module Frontend.AdminLoginPage)
            ~props:{csrf_token; error= Some "Invalid form submission."} )
