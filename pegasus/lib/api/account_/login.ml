let get_handler =
  Xrpc.handler (fun ctx ->
      let _redirect_url =
        Dream.query ctx.req "redirect_url" |> Option.value ~default:"/"
      in
      (* render login page with
          [ ("redirect_url", `String redirect_url)
          ; ("error", `Null) ]
          *)
      Dream.html "" )

let post_handler =
  Xrpc.handler (fun ctx ->
      let%lwt form = Dream.form ctx.req in
      match form with
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
              (* render login page with
                  [ ("error", `String "Invalid credentials")
                  ; ("redirect_url", `String redirect_url) ]
                  *)
              Dream.html ~status:`Unauthorized ""
          | Some {did; _} ->
              let%lwt () = Dream.invalidate_session ctx.req in
              let%lwt () = Dream.set_session_field ctx.req "did" did in
              Dream.redirect ctx.req redirect_url )
      | _ ->
          Errors.invalid_request "invalid request body" )
