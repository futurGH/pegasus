  let unwrap m =
    match%lwt m with
    | Ok x ->
        Lwt.return x
    | Error e ->
        raise (Caqti_error.Exn e)

  (* unwraps an Lwt result, raising an exception if there's an error *)
  let ( let$! ) m f =
    match%lwt m with Ok x -> f x | Error e -> raise (Caqti_error.Exn e)

  (* unwraps an Lwt result, raising an exception if there's an error *)
  let ( >$! ) m f =
    match%lwt m with
    | Ok x ->
        Lwt.return (f x)
    | Error e ->
        raise (Caqti_error.Exn e)

  let at_uri_regexp =
    Re.Pcre.re
      {|^at:\/\/([a-zA-Z0-9._:%-]+)(?:\/([a-zA-Z0-9-.]+)(?:\/([a-zA-Z0-9._~:@!$&%')(*+,;=-]+))?)?(?:#(\/[a-zA-Z0-9._~:@!$&%')(*+,;=\-[\]\/\\]*))?$|}
    |> Re.compile

  type at_uri =
    {repo: string; collection: string; rkey: string; fragment: string option}

  let parse_at_uri uri =
    match Re.exec_opt at_uri_regexp uri with
    | None ->
        None
    | Some m -> (
      try
        Some
          { repo= Re.Group.get m 1
          ; collection= Re.Group.get m 2
          ; rkey= Re.Group.get m 3
          ; fragment= Re.Group.get_opt m 4 }
      with _ -> None )

  let make_at_uri ~repo ~collection ~rkey ~fragment =
    Printf.sprintf "at://%s/%s/%s%s" repo collection rkey
      (Option.value ~default:"" fragment)

  let nsid_authority nsid =
    match String.rindex_opt nsid '.' with
    | None ->
        nsid
    | Some idx ->
        String.sub nsid 0 idx
