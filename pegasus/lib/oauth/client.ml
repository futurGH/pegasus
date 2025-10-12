type metadata =
  { client_id: string
  ; client_name: string option
  ; client_uri: string
  ; redirect_uris: string list
  ; grant_types: string list
  ; response_types: string list
  ; scope: string
  ; token_endpoint_auth_method: string
  ; application_type: string
  ; dpop_bound_access_tokens: bool
  ; jwks_uri: string option
  ; jwks: Yojson.Safe.t option }

let fetch_client_metadata client_id =
  let%lwt {status; _}, res =
    Cohttp_lwt_unix.Client.get (Uri.of_string client_id)
  in
  if status <> `OK then
    let%lwt () = Cohttp_lwt.Body.drain_body res in
    Errors.invalid_request "client metadata not found"
  else
    let%lwt body = Cohttp_lwt.Body.to_string res in
    let json = Yojson.Safe.from_string body in
    let open Yojson.Safe.Util in
    let metadata =
      { client_id= json |> member "client_id" |> to_string
      ; client_name= json |> member "client_name" |> to_string_option
      ; client_uri= json |> member "client_uri" |> to_string
      ; redirect_uris=
          json |> member "redirect_uris" |> to_list |> List.map to_string
      ; grant_types=
          json |> member "grant_types" |> to_list |> List.map to_string
      ; response_types=
          json |> member "response_types" |> to_list |> List.map to_string
      ; scope= json |> member "scope" |> to_string
      ; token_endpoint_auth_method=
          json |> member "token_endpoint_auth_method" |> to_string
      ; application_type= json |> member "application_type" |> to_string
      ; dpop_bound_access_tokens=
          json |> member "dpop_bound_access_tokens" |> to_bool
      ; jwks_uri= json |> member "jwks_uri" |> to_string_option
      ; jwks= json |> member "jwks" |> to_option Fun.id }
    in
    if metadata.client_id <> client_id then
      Errors.invalid_request "client_id mismatch"
    else
      let scopes = String.split_on_char ' ' metadata.scope in
      if not (List.mem "atproto" scopes) then
        Errors.invalid_request "scope must include 'atproto'"
      else
        List.iter
          (fun uri ->
            let u = Uri.of_string uri in
            match Uri.scheme u with
            | Some "https" ->
                ()
            | Some "http"
              when Uri.host u = Some "127.0.0.1" || Uri.host u = Some "[::1]" ->
                ()
            | _ ->
                Errors.invalid_request ("invalid redirect_uri: " ^ uri) )
          metadata.redirect_uris ;
      Lwt.return metadata
