let format_date timestamp_ms =
  let ts = float_of_int timestamp_ms /. 1000.0 in
  let dt = Timedesc.of_timestamp_float_s_exn ts in
  Format.asprintf "%a" (Timedesc.pp ~format:"{year}/{mon:0X}/{day:0X}" ()) dt

let get_client_host client_id =
  let uri = Uri.of_string client_id in
  Uri.host uri |> Option.value ~default:client_id

let get_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Dream.redirect ctx.req "/account/login"
      | Some did -> (
          let%lwt current_user, logged_in_users =
            Session.list_logged_in_actors ctx.req ctx.db
          in
          match%lwt Data_store.get_actor_by_identifier did ctx.db with
          | None ->
              Dream.redirect ctx.req "/account/login"
          | Some actor ->
              let current_user =
                Option.value
                  ~default:
                    {did= actor.did; handle= actor.handle; avatar_data_uri= None}
                  current_user
              in
              let csrf_token = Dream.csrf_token ctx.req in
              let%lwt clients =
                Oauth.Queries.get_distinct_clients_by_did ctx.db did
              in
              let%lwt authorized_apps =
                Lwt_list.filter_map_s
                  (fun (client_id, _last_refreshed) ->
                    try%lwt
                      let%lwt metadata =
                        Oauth.Client.fetch_client_metadata client_id
                      in
                      let app : Frontend.AccountPermissionsPage.authorized_app =
                        { client_id
                        ; client_name= metadata.client_name
                        ; client_host= get_client_host client_id }
                      in
                      Lwt.return_some app
                    with _ ->
                      let app : Frontend.AccountPermissionsPage.authorized_app =
                        { client_id
                        ; client_name= None
                        ; client_host= get_client_host client_id }
                      in
                      Lwt.return_some app )
                  clients
              in
              let%lwt device_rows =
                Oauth.Queries.get_distinct_devices_by_did ctx.db did
              in
              let current_ip = Util.request_ip ctx.req in
              let current_ua = Dream.header ctx.req "User-Agent" in
              let devices =
                List.map
                  (fun (last_ip, last_user_agent, last_refreshed_ms) ->
                    let is_current =
                      last_ip = current_ip && last_user_agent = current_ua
                    in
                    let last_refreshed_at = format_date last_refreshed_ms in
                    ( {last_ip; last_user_agent; last_refreshed_at; is_current}
                      : Frontend.AccountPermissionsPage.device ) )
                  device_rows
              in
              Util.render_html ~title:"Permissions"
                (module Frontend.AccountPermissionsPage)
                ~props:
                  { current_user
                  ; logged_in_users
                  ; csrf_token
                  ; authorized_apps
                  ; devices } ) )

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Dream.redirect ctx.req "/account/login"
      | Some did -> (
        match%lwt Dream.form ctx.req with
        | `Ok fields -> (
            let action = List.assoc_opt "action" fields in
            match action with
            | Some "revoke_app" -> (
                let client_id = List.assoc_opt "client_id" fields in
                match client_id with
                | Some client_id ->
                    let%lwt () =
                      Oauth.Queries.delete_oauth_tokens_by_client ctx.db ~did
                        ~client_id
                    in
                    Oauth.Dpop.revoke_tokens_for_did did ;
                    Dream.redirect ctx.req "/account/permissions"
                | None ->
                    Dream.redirect ctx.req "/account/permissions" )
            | Some "sign_out_device" ->
                let last_ip =
                  List.assoc_opt "last_ip" fields |> Option.value ~default:""
                in
                let last_user_agent =
                  match List.assoc_opt "last_user_agent" fields with
                  | Some "" ->
                      None
                  | other ->
                      other
                in
                let%lwt () =
                  Oauth.Queries.delete_oauth_tokens_by_device ctx.db ~did
                    ~last_ip ~last_user_agent
                in
                Oauth.Dpop.revoke_tokens_for_did did ;
                Dream.redirect ctx.req "/account/permissions"
            | _ ->
                Dream.redirect ctx.req "/account/permissions" )
        | _ ->
            Dream.redirect ctx.req "/account/permissions" ) )
