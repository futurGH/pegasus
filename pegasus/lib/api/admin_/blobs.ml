let format_size bytes =
  if bytes < 1024 then Printf.sprintf "%d B" bytes
  else if bytes < 1024 * 1024 then
    Printf.sprintf "%.1f KB" (float_of_int bytes /. 1024.0)
  else if bytes < 1024 * 1024 * 1024 then
    Printf.sprintf "%.1f MB" (float_of_int bytes /. 1024.0 /. 1024.0)
  else
    Printf.sprintf "%.1f GB" (float_of_int bytes /. 1024.0 /. 1024.0 /. 1024.0)

let format_date timestamp_ms =
  let ts = float_of_int timestamp_ms /. 1000.0 in
  let dt = Timedesc.of_timestamp_float_s_exn ts in
  Format.asprintf "%a"
    (Timedesc.pp
       ~format:"{year}-{mon:0X}-{day:0X}, {12hour:0X}:{min:0X} {am/pm:XX}" () )
    dt

let get_blob_size ~did ~cid ~storage : int Lwt.t =
  match%lwt Blob_store.get ~did ~cid ~storage with
  | Some data ->
      Lwt.return (Bytes.length data)
  | None ->
      Lwt.return 0

let list_all_blobs ~limit ~cursor db =
  let%lwt actors = Data_store.list_actors ~limit:1000 db in
  let%lwt all_blobs =
    Lwt_list.fold_left_s
      (fun acc (actor : Data_store.Types.actor) ->
        try%lwt
          let%lwt user_db = User_store.connect actor.did in
          let%lwt blobs = User_store.list_blobs user_db ~limit:100 ~cursor:"" in
          let%lwt blob_metadata =
            Lwt_list.map_s
              (fun cid ->
                let%lwt blob_opt = User_store.get_blob_metadata user_db cid in
                match blob_opt with
                | Some blob ->
                    let%lwt size =
                      get_blob_size ~did:actor.did ~cid ~storage:blob.storage
                    in
                    Lwt.return
                      (Some
                         ( actor.did
                         , actor.handle
                         , actor.created_at
                         , cid
                         , blob.mimetype
                         , blob.storage
                         , size ) )
                | None ->
                    Lwt.return None )
              blobs
          in
          let filtered_blobs = List.filter_map (fun x -> x) blob_metadata in
          Lwt.return (acc @ filtered_blobs)
        with _ -> Lwt.return acc )
      [] actors
  in
  let sorted_blobs =
    List.sort
      (fun (did1, _, _, cid1, _, _, _) (did2, _, _, cid2, _, _, _) ->
        let did_cmp = String.compare did1 did2 in
        if did_cmp <> 0 then did_cmp else Cid.compare cid1 cid2 )
      all_blobs
  in
  let filtered =
    if cursor = "" then sorted_blobs
    else
      List.filter
        (fun (did, _, _, cid, _, _, _) ->
          let key = did ^ ":" ^ Cid.to_string cid in
          key > cursor )
        sorted_blobs
  in
  (* take limit + 1 for pagination *)
  let page =
    if List.length filtered > limit then
      List.filteri (fun i _ -> i < limit) filtered
    else filtered
  in
  let has_more = List.length filtered > limit in
  let next_cursor =
    if has_more then
      match List.rev page with
      | (did, _, _, cid, _, _, _) :: _ ->
          Some (did ^ ":" ^ Cid.to_string cid)
      | [] ->
          None
    else None
  in
  Lwt.return (page, next_cursor)

let blob_to_view (did, handle, created_at, cid, mimetype, storage, size) :
    Frontend.AdminBlobsPage.blob =
  { did
  ; handle
  ; user_created_at= format_date created_at
  ; cid= Cid.to_string cid
  ; mimetype
  ; storage= Blob_store.storage_to_string storage
  ; size= format_size size }

let get_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | false ->
          Dream.redirect ctx.req "/admin/login"
      | true ->
          let cursor =
            Dream.query ctx.req "cursor" |> Option.value ~default:""
          in
          let limit = 50 in
          let%lwt blobs, next_cursor = list_all_blobs ~limit ~cursor ctx.db in
          let blobs = List.map blob_to_view blobs in
          let csrf_token = Dream.csrf_token ctx.req in
          Util.render_html ~title:"Admin / Blobs"
            (module Frontend.AdminBlobsPage)
            ~props:
              { blobs
              ; csrf_token
              ; cursor
              ; next_cursor
              ; error= None
              ; success= None } )

let view_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | false ->
          Dream.redirect ctx.req "/admin/login"
      | true -> (
          let did = Dream.query ctx.req "did" |> Option.value ~default:"" in
          let cid_str = Dream.query ctx.req "cid" |> Option.value ~default:"" in
          let download = Dream.query ctx.req "download" |> Option.is_some in
          match Cid.of_string cid_str with
          | Ok cid -> (
            try%lwt
              let%lwt user_db = User_store.connect did in
              match%lwt User_store.get_blob user_db cid with
              | Some blob ->
                  let content_disposition =
                    if download then
                      "attachment; filename=\"" ^ Cid.to_string cid ^ "\""
                    else "inline"
                  in
                  Dream.respond
                    ~headers:
                      [ ("Content-Type", blob.mimetype)
                      ; ("Content-Disposition", content_disposition)
                      ; ("Cache-Control", "public, max-age=31536000") ]
                    (Bytes.to_string blob.data)
              | None ->
                  Dream.respond ~status:`Not_Found "Blob not found"
            with e ->
              Dream.respond ~status:`Internal_Server_Error
                ("Error: " ^ Printexc.to_string e) )
          | Error _ ->
              Dream.respond ~status:`Bad_Request "Invalid CID" ) )

let post_handler =
  Xrpc.handler (fun ctx ->
      match%lwt Session.is_admin_authenticated ctx.req with
      | false ->
          Dream.redirect ctx.req "/admin/login"
      | true -> (
          let csrf_token = Dream.csrf_token ctx.req in
          let render_page ?error ?success () =
            let cursor =
              Dream.query ctx.req "cursor" |> Option.value ~default:""
            in
            let limit = 50 in
            let%lwt blobs, next_cursor = list_all_blobs ~limit ~cursor ctx.db in
            let blobs = List.map blob_to_view blobs in
            Util.render_html ~title:"Admin / Blobs"
              (module Frontend.AdminBlobsPage)
              ~props:{blobs; csrf_token; cursor; next_cursor; error; success}
          in
          match%lwt Dream.form ctx.req with
          | `Ok fields -> (
              let action = List.assoc_opt "action" fields in
              let did =
                List.assoc_opt "did" fields |> Option.value ~default:""
              in
              let cid_str =
                List.assoc_opt "cid" fields |> Option.value ~default:""
              in
              match action with
              | Some "delete_blob" -> (
                match Cid.of_string cid_str with
                | Ok cid -> (
                  try%lwt
                    let%lwt user_db = User_store.connect did in
                    let%lwt () = User_store.delete_blob user_db cid in
                    render_page ~success:"Blob deleted successfully." ()
                  with e ->
                    render_page
                      ~error:("Failed to delete blob: " ^ Printexc.to_string e)
                      () )
                | Error _ ->
                    render_page ~error:"Invalid blob CID." () )
              | _ ->
                  render_page ~error:"Invalid action." () )
          | _ ->
              render_page ~error:"Invalid form submission." () ) )
