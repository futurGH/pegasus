open Lexicons.Com.Atproto.Repo.PutRecord.Main

let calc_key_did ctx = Some (Auth.get_authed_did_exn ctx.Xrpc.auth)

let calc_points_put _ctx = 2

let handler =
  Xrpc.handler ~auth:Authorization
    ~rate_limits:
      [ Shared
          { name= "repo-write-hour"
          ; calc_key= Some calc_key_did
          ; calc_points= Some calc_points_put }
      ; Shared
          { name= "repo-write-day"
          ; calc_key= Some calc_key_did
          ; calc_points= Some calc_points_put } ]
    (fun ctx ->
      let%lwt input = Xrpc.parse_body ctx.req input_of_yojson in
      (* assert create and update because we don't know which will end up happening *)
      Auth.assert_repo_scope ctx.auth ~collection:input.collection
        ~action:Oauth.Scopes.Create ;
      Auth.assert_repo_scope ctx.auth ~collection:input.collection
        ~action:Oauth.Scopes.Update ;
      let%lwt did = Xrpc.resolve_repo_did_authed ctx input.repo in
      let%lwt repo = Repository.load did in
      let write : Repository.repo_write =
        match input.swap_record with
        | Some swap_record ->
            Update
              { type'= Repository.Write_op.update
              ; collection= input.collection
              ; rkey= input.rkey
              ; value=
                  Result.get_ok
                  @@ Repository.Lex.repo_record_of_yojson input.record
              ; swap_record= Some (Cid.as_cid swap_record) }
        | None ->
            Create
              { type'= Repository.Write_op.create
              ; collection= input.collection
              ; rkey= Some input.rkey
              ; value=
                  Result.get_ok
                  @@ Repository.Lex.repo_record_of_yojson input.record }
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
