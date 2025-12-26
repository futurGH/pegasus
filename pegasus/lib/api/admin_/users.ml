let format_date timestamp_ms =
  let ts = float_of_int timestamp_ms /. 1000.0 in
  let dt = Timedesc.of_timestamp_float_s_exn ts in
  Format.asprintf "%a"
    (Timedesc.pp
       ~format:"{year}-{mon:0X}-{day:0X}, {12hour:0X}:{min:0X} {am/pm:XX}" () )
    dt

let actor_to_view (actor : Data_store.Types.actor) :
    Frontend.AdminUsersPage.actor =
  { did= actor.did
  ; handle= actor.handle
  ; email= actor.email
  ; email_confirmed= actor.email_confirmed_at <> None
  ; created_at= format_date actor.created_at
  ; deactivated= actor.deactivated_at <> None }

let get_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | false ->
          Dream.redirect ctx.req "/admin/login"
      | true ->
          let filter =
            Dream.query ctx.req "filter" |> Option.value ~default:""
          in
          let cursor =
            Dream.query ctx.req "cursor" |> Option.value ~default:""
          in
          let limit = 20 in
          let%lwt actors =
            Data_store.list_actors_filtered ~filter ~cursor ~limit:(limit + 1)
              ctx.db
          in
          let has_more = List.length actors > limit in
          let actors =
            if has_more then List.filteri (fun i _ -> i < limit) actors
            else actors
          in
          let next_cursor =
            if has_more then
              match List.rev actors with
              | last :: _ ->
                  Some last.did
              | [] ->
                  None
            else None
          in
          let actors = List.map actor_to_view actors in
          let csrf_token = Dream.csrf_token ctx.req in
          let hostname = Env.hostname in
          Util.render_html ~title:"Admin / Users"
            (module Frontend.AdminUsersPage)
            ~props:
              { actors
              ; csrf_token
              ; filter
              ; cursor
              ; next_cursor
              ; hostname
              ; error= None
              ; success= None } )

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | false ->
          Dream.redirect ctx.req "/admin/login"
      | true -> (
          let csrf_token = Dream.csrf_token ctx.req in
          let hostname = Env.hostname in
          let render_page ?error ?success () =
            let filter =
              Dream.query ctx.req "filter" |> Option.value ~default:""
            in
            let cursor =
              Dream.query ctx.req "cursor" |> Option.value ~default:""
            in
            let limit = 20 in
            let%lwt actors =
              Data_store.list_actors_filtered ~filter ~cursor ~limit:(limit + 1)
                ctx.db
            in
            let has_more = List.length actors > limit in
            let actors =
              if has_more then List.filteri (fun i _ -> i < limit) actors
              else actors
            in
            let actors = List.map actor_to_view actors in
            let next_cursor =
              if has_more then
                match List.rev actors with
                | last :: _ ->
                    Some last.did
                | [] ->
                    None
              else None
            in
            Util.render_html ~title:"Admin / Users"
              (module Frontend.AdminUsersPage)
              ~props:
                { actors
                ; csrf_token
                ; filter
                ; cursor
                ; next_cursor
                ; hostname
                ; error
                ; success }
          in
          match%lwt Dream.form ctx.req with
          | `Ok fields -> (
              let action = List.assoc_opt "action" fields in
              let did =
                List.assoc_opt "did" fields |> Option.value ~default:""
              in
              match action with
              | Some "create_account" -> (
                  let email =
                    List.assoc_opt "email" fields
                    |> Option.value ~default:"" |> String.lowercase_ascii
                  in
                  let handle_input =
                    List.assoc_opt "handle" fields |> Option.value ~default:""
                  in
                  let password =
                    List.assoc_opt "password" fields |> Option.value ~default:""
                  in
                  if
                    String.length email = 0
                    || String.length handle_input = 0
                    || String.length password = 0
                  then render_page ~error:"All fields are required." ()
                  else
                    let hostname_suffix = "." ^ hostname in
                    let handle =
                      if String.contains handle_input '.' then handle_input
                      else handle_input ^ hostname_suffix
                    in
                    match Util.validate_handle handle with
                    | Error (InvalidFormat e)
                    | Error (TooLong e)
                    | Error (TooShort e) ->
                        render_page ~error:("Handle " ^ e) ()
                    | Ok _ -> (
                      match%lwt
                        Data_store.get_actor_by_identifier email ctx.db
                      with
                      | Some _ ->
                          render_page ~error:"Email already in use." ()
                      | None -> (
                        match%lwt
                          Data_store.get_actor_by_identifier handle ctx.db
                        with
                        | Some _ ->
                            render_page ~error:"Handle already in use." ()
                        | None -> (
                            let signing_key, signing_pubkey =
                              Kleidos.K256.generate_keypair ()
                            in
                            let sk_did =
                              Kleidos.K256.pubkey_to_did_key signing_pubkey
                            in
                            match%lwt
                              Plc.submit_genesis Env.rotation_key sk_did handle
                            with
                            | Error e ->
                                render_page
                                  ~error:("Failed to create DID: " ^ e)
                                  ()
                            | Ok new_did ->
                                let sk_priv_mk =
                                  Kleidos.K256.privkey_to_multikey signing_key
                                in
                                let%lwt () =
                                  Data_store.create_actor ~did:new_did ~handle
                                    ~email ~password ~signing_key:sk_priv_mk
                                    ctx.db
                                in
                                let () =
                                  Util.mkfile_p
                                    (Util.Constants.user_db_filepath new_did)
                                    ~perm:0o644
                                in
                                let%lwt repo =
                                  Repository.load ~create:true new_did
                                in
                                let%lwt _ =
                                  Repository.put_initial_commit repo
                                in
                                let%lwt _ =
                                  Sequencer.sequence_identity ctx.db
                                    ~did:new_did ~handle ()
                                in
                                let%lwt _ =
                                  Sequencer.sequence_account ctx.db ~did:new_did
                                    ~active:true ()
                                in
                                render_page
                                  ~success:("Account created: " ^ handle)
                                  () ) ) ) )
              | Some "change_handle" -> (
                  let handle =
                    List.assoc_opt "handle" fields |> Option.value ~default:""
                  in
                  match%lwt
                    Identity.UpdateHandle.update_handle ~did ~handle ctx.db
                  with
                  | Ok () ->
                      render_page ~success:"Handle updated." ()
                  | Error (InvalidFormat e)
                  | Error (TooLong e)
                  | Error (TooShort e) ->
                      render_page ~error:("Handle " ^ e) ()
                  | Error HandleTaken ->
                      render_page ~error:"Handle already taken" ()
                  | Error (InternalServerError _) ->
                      render_page ~error:"Internal server error" () )
              | Some "change_email" -> (
                  let email =
                    List.assoc_opt "email" fields
                    |> Option.value ~default:"" |> String.lowercase_ascii
                  in
                  match%lwt Data_store.get_actor_by_identifier email ctx.db with
                  | Some existing when existing.did <> did ->
                      render_page ~error:"Email already in use." ()
                  | _ ->
                      let%lwt () = Data_store.update_email ~did ~email ctx.db in
                      render_page ~success:"Email updated." () )
              | Some "change_password" ->
                  let password =
                    List.assoc_opt "password" fields |> Option.value ~default:""
                  in
                  let%lwt () =
                    Data_store.update_password ~did ~password ctx.db
                  in
                  render_page ~success:"Password updated." ()
              | Some "send_password_reset" -> (
                match%lwt Data_store.get_actor_by_identifier did ctx.db with
                | None ->
                    render_page ~error:"Account not found." ()
                | Some actor ->
                    let%lwt () =
                      Server.RequestPasswordReset.request_password_reset actor
                        ctx.db
                    in
                    render_page ~success:"Password reset email sent." () )
              | Some "deactivate" ->
                  let%lwt _ =
                    Server.DeactivateAccount.deactivate_account ~did ctx.db
                  in
                  render_page ~success:"Account deactivated." ()
              | Some "reactivate" ->
                  let%lwt () = Data_store.activate_actor did ctx.db in
                  let%lwt _ =
                    Sequencer.sequence_account ctx.db ~did ~active:true
                      ~status:`Active ()
                  in
                  render_page ~success:"Account reactivated." ()
              | Some "delete" ->
                  let%lwt _ = Server.DeleteAccount.delete_account ~did ctx.db in
                  render_page ~success:"Account deleted." ()
              | _ ->
                  render_page ~error:"Invalid action." () )
          | _ ->
              render_page ~error:"Invalid form submission." () ) )
