let invite_to_view (invite : Data_store.Types.invite_code) :
    Frontend.AdminInvitesPage.invite =
  {code= invite.code; did= invite.did; remaining= invite.remaining}

let get_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | false ->
          Dream.redirect ctx.req "/admin/login"
      | true ->
          let%lwt invites = Data_store.list_invites ~limit:100 ctx.db in
          let invites = List.map invite_to_view invites in
          let csrf_token = Dream.csrf_token ctx.req in
          Util.render_html ~title:"Admin / Invite Codes"
            (module Frontend.AdminInvitesPage)
            ~props:{invites; csrf_token; error= None; success= None} )

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | false ->
          Dream.redirect ctx.req "/admin/login"
      | true -> (
          let csrf_token = Dream.csrf_token ctx.req in
          let render_page ?error ?success () =
            let%lwt invites = Data_store.list_invites ~limit:100 ctx.db in
            let invites = List.map invite_to_view invites in
            Util.render_html ~title:"Admin / Invite Codes"
              (module Frontend.AdminInvitesPage)
              ~props:{invites; csrf_token; error; success}
          in
          match%lwt Dream.form ctx.req with
          | `Ok fields -> (
              let action = List.assoc_opt "action" fields in
              let code =
                List.assoc_opt "code" fields |> Option.value ~default:""
              in
              match action with
              | Some "create_invite" -> (
                  let did =
                    List.assoc_opt "did" fields |> Option.value ~default:"admin"
                  in
                  let remaining =
                    List.assoc_opt "remaining" fields
                    |> Option.value ~default:"1" |> int_of_string_opt
                    |> Option.value ~default:1
                  in
                  let new_code =
                    List.assoc_opt "new_code" fields |> Option.value ~default:""
                  in
                  let code =
                    if String.length new_code > 0 then new_code
                    else Server.CreateInviteCode.generate_code did
                  in
                  match%lwt Data_store.get_invite ~code ctx.db with
                  | Some _ ->
                      render_page ~error:"Invite code already exists." ()
                  | None ->
                      let%lwt () =
                        Data_store.create_invite ~code ~did ~remaining ctx.db
                      in
                      render_page ~success:("Invite code created: " ^ code) () )
              | Some "update_invite" ->
                  let did =
                    List.assoc_opt "did" fields |> Option.value ~default:"admin"
                  in
                  let remaining =
                    List.assoc_opt "remaining" fields
                    |> Option.value ~default:"1" |> int_of_string_opt
                    |> Option.value ~default:1
                  in
                  let%lwt () =
                    Data_store.update_invite ~code ~did ~remaining ctx.db
                  in
                  render_page ~success:"Invite code updated." ()
              | Some "delete_invite" ->
                  let%lwt () = Data_store.delete_invite ~code ctx.db in
                  render_page ~success:"Invite code deleted." ()
              | _ ->
                  render_page ~error:"Invalid action." () )
          | _ ->
              render_page ~error:"Invalid form submission." () ) )
