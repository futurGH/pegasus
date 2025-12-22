open Types

let fetch_client_metadata client_id : client_metadata Lwt.t =
  let%lwt {status; _}, res = Util.http_get (Uri.of_string client_id) in
  if status <> `OK then
    let%lwt () = Cohttp_lwt.Body.drain_body res in
    failwith
      (Printf.sprintf "client metadata not found; http %d"
         (Cohttp.Code.code_of_status status) )
  else
    let%lwt body = Cohttp_lwt.Body.to_string res in
    let json = Yojson.Safe.from_string body in
    let metadata =
      match client_metadata_of_yojson json with
      | Ok metadata ->
          metadata
      | Error err ->
          failwith err
    in
    if metadata.client_id <> Some client_id then failwith "client_id mismatch"
    else
      let scopes =
        String.split_on_char ' ' (Option.value metadata.scope ~default:"")
      in
      if not (List.mem "atproto" scopes) then
        failwith "scope must include 'atproto'"
      else
        List.iter
          (function
            | "authorization_code" | "refresh_token" ->
                ()
            | grant ->
                failwith ("invalid grant type: " ^ grant) )
          metadata.grant_types ;
      List.iter
        (fun uri ->
          let u = Uri.of_string uri in
          let host = Uri.host u in
          match Uri.scheme u with
          | Some "https" when host <> Some "localhost" ->
              ()
          | Some "http" when host = Some "127.0.0.1" || host = Some "[::1]" ->
              ()
          | _ ->
              failwith ("invalid redirect_uri: " ^ uri) )
        metadata.redirect_uris ;
      Lwt.return metadata
