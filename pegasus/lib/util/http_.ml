let rec get ?(max_redirects = 5) ?(no_drain = false) ?headers uri =
  let ua = "pegasus (" ^ Env.host_endpoint ^ ")" in
  let headers =
    match headers with
    | Some headers ->
        Http.Header.add_unless_exists headers "User-Agent" ua
    | None ->
        Http.Header.of_list [("User-Agent", ua)]
  in
  let%lwt ans = Cohttp_lwt_unix.Client.get ~headers uri in
  follow_redirect ~max_redirects ~no_drain uri ans

and follow_redirect ~max_redirects ~no_drain request_uri (response, body) =
  let status = Http.Response.status response in
  (* the unconsumed body would otherwise leak memory *)
  let%lwt () =
    if status <> `OK && not no_drain then Cohttp_lwt.Body.drain_body body
    else Lwt.return_unit
  in
  match status with
  | `Permanent_redirect | `Moved_permanently ->
      handle_redirect ~permanent:true ~max_redirects request_uri response
  | `Found | `Temporary_redirect ->
      handle_redirect ~permanent:false ~max_redirects request_uri response
  | _ ->
      Lwt.return (response, body)

and handle_redirect ~permanent ~max_redirects request_uri response =
  if max_redirects <= 0 then failwith "too many redirects"
  else
    let headers = Http.Response.headers response in
    let location = Http.Header.get headers "location" in
    match location with
    | None ->
        failwith "redirection without Location header"
    | Some url ->
        let uri = Uri.of_string url in
        let%lwt () =
          if permanent then
            Logs_lwt.warn (fun m ->
                m "Permanent redirection from %s to %s"
                  (Uri.to_string request_uri)
                  url )
          else Lwt.return_unit
        in
        get uri ~max_redirects:(max_redirects - 1)

let copy_query req = Dream.all_queries req |> List.map (fun (k, v) -> (k, [v]))

let make_headers headers =
  List.fold_left
    (fun headers (k, v) ->
      match v with
      | Some value ->
          Http.Header.add headers k value
      | None ->
          headers )
    (Http.Header.init ()) headers
