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
              let is_plc = String.starts_with ~prefix:"did:plc:" did in
              let%lwt credentials_json =
                if is_plc then
                  match%lwt Data_store.get_actor_by_identifier did ctx.db with
                  | Some {handle; signing_key; _} ->
                      Lwt.return_some
                        ( Plc.get_recommended_credentials ~handle ~signing_key ()
                        |> Plc.credentials_to_yojson
                        |> Yojson.Safe.pretty_to_string )
                  | None ->
                      Lwt.return_none
                else Lwt.return_none
              in
              Util.render_html ~title:"Identity"
                (module Frontend.AccountIdentityPage)
                ~props:
                  { current_user
                  ; logged_in_users
                  ; csrf_token
                  ; is_plc
                  ; credentials_json
                  ; error= None
                  ; success= None } ) )

type post_response = {error: string option [@default None]}
[@@deriving yojson {strict= false}]

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.Raw.get_current_did ctx.req with
      | None ->
          Dream.json ~status:`Unauthorized
            (Yojson.Safe.to_string
               (post_response_to_yojson {error= Some "Not authenticated"}) )
      | Some did -> (
          if not (String.starts_with ~prefix:"did:plc:" did) then
            Dream.json ~status:`Bad_Request
              (Yojson.Safe.to_string
                 (post_response_to_yojson
                    {error= Some "Identity management is only for did:plc"} ) )
          else
            match%lwt Dream.form ctx.req with
            | `Ok fields -> (
                let action = List.assoc_opt "action" fields in
                match action with
                | Some "submit" -> (
                    let credentials_str =
                      List.assoc_opt "credentials" fields
                      |> Option.value ~default:"{}"
                    in
                    match
                      Yojson.Safe.from_string credentials_str
                      |> Plc.credentials_of_yojson
                    with
                    | Error e ->
                        Dream.json ~status:`Bad_Request
                          (Yojson.Safe.to_string
                             (post_response_to_yojson
                                {error= Some ("Invalid JSON: " ^ e)} ) )
                    | Ok credentials -> (
                      match%lwt Plc.get_audit_log did with
                      | Error e ->
                          Dream.json ~status:`Internal_Server_Error
                            (Yojson.Safe.to_string
                               (post_response_to_yojson
                                  {error= Some ("Failed to get audit log: " ^ e)} ) )
                      | Ok log -> (
                          let latest = Mist.Util.last log |> Option.get in
                          let unsigned_op : Plc.unsigned_operation =
                            Operation
                              { type'= "plc_operation"
                              ; rotation_keys= credentials.rotation_keys
                              ; verification_methods=
                                  credentials.verification_methods
                              ; also_known_as= credentials.also_known_as
                              ; services= credentials.services
                              ; prev= Some latest.cid }
                          in
                          let signed_op =
                            Plc.sign_operation Env.rotation_key unsigned_op
                          in
                          match%lwt
                            Data_store.get_actor_by_identifier did ctx.db
                          with
                          | None ->
                              Dream.json ~status:`Internal_Server_Error
                                (Yojson.Safe.to_string
                                   (post_response_to_yojson
                                      {error= Some "Actor not found"} ) )
                          | Some actor -> (
                            match
                              Plc.validate_operation ~handle:actor.handle
                                ~signing_key:actor.signing_key signed_op
                            with
                            | Error e ->
                                Dream.json ~status:`Bad_Request
                                  (Yojson.Safe.to_string
                                     (post_response_to_yojson {error= Some e}) )
                            | Ok () -> (
                              match%lwt Plc.submit_operation did signed_op with
                              | Ok () ->
                                  let%lwt _ =
                                    Sequencer.sequence_identity ctx.db ~did ()
                                  in
                                  let%lwt _ = Id_resolver.Did.resolve did in
                                  Dream.json ~status:`OK
                                    (Yojson.Safe.to_string
                                       (post_response_to_yojson {error= None}) )
                              | Error (_status, msg) ->
                                  Dream.json ~status:`Bad_Request
                                    (Yojson.Safe.to_string
                                       (post_response_to_yojson
                                          { error=
                                              Some
                                                ( "The directory returned an \
                                                   error: " ^ msg ) } ) ) ) ) )
                      ) )
                | _ ->
                    Dream.json ~status:`Bad_Request
                      (Yojson.Safe.to_string
                         (post_response_to_yojson
                            {error= Some "Invalid action"} ) ) )
            | _ ->
                Dream.json ~status:`Bad_Request
                  (Yojson.Safe.to_string
                     (post_response_to_yojson
                        {error= Some "Invalid form submission"} ) ) ) )
