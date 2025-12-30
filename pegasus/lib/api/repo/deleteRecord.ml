open Lexicons.Com_atproto_repo_deleteRecord.Main

let calc_key_did ctx = Some (Auth.get_authed_did_exn ctx.Xrpc.auth)

let calc_points_delete _ctx = 1

let handler =
  Xrpc.handler ~auth:Authorization
    ~rate_limits:
      [ Shared
          { name= "repo-write-hour"
          ; calc_key= Some calc_key_did
          ; calc_points= Some calc_points_delete }
      ; Shared
          { name= "repo-write-day"
          ; calc_key= Some calc_key_did
          ; calc_points= Some calc_points_delete } ]
    (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req input_of_yojson in
      Auth.assert_repo_scope ctx.auth ~collection:input.collection
        ~action:Oauth.Scopes.Delete ;
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      let%lwt repo = Repository.load did in
      let write : Repository.repo_write =
        Delete
          { type'= Repository.Write_op.delete
          ; collection= input.collection
          ; rkey= input.rkey
          ; swap_record= Option.map Cid.as_cid input.swap_record }
      in
      let%lwt {commit= commit_cid, {rev; _}; results} =
        Repository.apply_writes repo [write]
          (Option.map Cid.as_cid input.swap_commit)
      in
      match List.hd results with
      | Delete _ ->
          Dream.json @@ Yojson.Safe.to_string
          @@ output_to_yojson
               {commit= Some {cid= Cid.to_string commit_cid; rev}}
      | _ ->
          Errors.invalid_request "unexpected create or update result" )
