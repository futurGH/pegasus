open Types

let reverse_domain domain =
  String.split_on_char '.' domain |> List.rev |> String.concat "."

(* checks a redirect uri a client registers. web clients redirect to https, or to
   loopback over http. native apps may also use a private-use uri scheme (rfc 8252 s.7.1)
   (the client_id's hostname reversed, followed by a single slash) *)
let validate_redirect_uri ~client_id ~application_type uri =
  let invalid reason =
    failwith (Printf.sprintf "invalid redirect_uri %s: %s" uri reason)
  in
  let u = Uri.of_string uri in
  let host = Uri.host u in
  match Option.map String.lowercase_ascii (Uri.scheme u) with
  | Some "https" when host <> Some "localhost" ->
      ()
  | Some "http"
    when host = Some "127.0.0.1" || host = Some "localhost"
         || host = Some "[::1]" ->
      ()
  | Some scheme when String.contains scheme '.' ->
      let expected =
        Uri.host (Uri.of_string client_id)
        |> Option.map (fun h -> reverse_domain (String.lowercase_ascii h))
      in
      if application_type <> "native" then
        invalid "private-use uri schemes are only allowed for native apps"
      else if Some scheme <> expected then
        invalid
          ( "private-use uri scheme must be the client_id's hostname reversed ("
          ^ Option.value expected ~default:"?"
          ^ ")" )
      else if
        String.starts_with ~prefix:(scheme ^ "://")
          (String.lowercase_ascii uri)
        || host <> None || Uri.userinfo u <> None || Uri.port u <> None
      then invalid ("must be in the form " ^ scheme ^ ":/<path>")
      else ()
  | _ ->
      invalid "unsupported scheme"

let fetch_client_metadata client_id : client_metadata Lwt.t =
  let%lwt {status; _}, res = Util.Http.get (Uri.of_string client_id) in
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
        (validate_redirect_uri ~client_id
           ~application_type:metadata.application_type )
        metadata.redirect_uris ;
      Lwt.return metadata
