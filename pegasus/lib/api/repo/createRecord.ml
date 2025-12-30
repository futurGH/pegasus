open Lexicons.Com_atproto_repo_createRecord.Main

let calc_key_did ctx = Some (Auth.get_authed_did_exn ctx.Xrpc.auth)

let calc_points_create _ctx = 3

let handler =
  Xrpc.handler ~auth:Authorization
    ~rate_limits:
      [ Shared
          { name= "repo-write-hour"
          ; calc_key= Some calc_key_did
          ; calc_points= Some calc_points_create }
      ; Shared
          { name= "repo-write-day"
          ; calc_key= Some calc_key_did
          ; calc_points= Some calc_points_create } ]
    (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req input_of_yojson in
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      Auth.assert_repo_scope ctx.auth ~collection:input.collection
        ~action:Oauth.Scopes.Create ;
      let%lwt repo = Repository.load did in
      let write : Repository.repo_write =
        Create
          { type'= Repository.Write_op.create
          ; collection= input.collection
          ; rkey= input.rkey
          ; value=
              Result.get_ok @@ Repository.Lex.repo_record_of_yojson input.record
          }
      in
      let%lwt {commit= commit_cid, {rev; _}; results} =
        Repository.apply_writes repo [write]
          (Option.map Cid.as_cid input.swap_commit)
      in
      match List.hd results with
      | Create {uri; cid; _} | Update {uri; cid; _} ->
          Dream.json @@ Yojson.Safe.to_string
          @@ output_to_yojson
               { uri
               ; cid= Cid.to_string cid
               ; commit= Some {cid= Cid.to_string commit_cid; rev}
               ; validation_status= Some "valid" }
      | _ ->
          Errors.invalid_request "unexpected delete result" )
