module StringMap = Dag_cbor.StringMap

type typed_json_ref = {type': string; ref: Cid.t; mime_type: string; size: int}

type untyped_json_ref = {cid: string; mime_type: string}

type json_ref = Typed of typed_json_ref | Untyped of untyped_json_ref

type t = {original: json_ref; ref: Cid.t; mime_type: string; size: int}

let of_yojson json =
  let open Yojson.Safe.Util in
  let assoc = to_assoc json in
  try
    if List.mem_assoc "$type" assoc then
      let type' = assoc |> List.assoc "$type" |> to_string in
      if type' = "blob" then
        let cid_str =
          assoc |> List.assoc "ref" |> to_assoc |> List.assoc "$link"
          |> to_string
        in
        let ref = Result.get_ok @@ Cid.of_string cid_str in
        let mime_type = assoc |> List.assoc "mimeType" |> to_string in
        let size = assoc |> List.assoc "size" |> to_int in
        Typed {type'; ref; mime_type; size}
      else invalid_arg "of_yojson: invalid blob ref $type"
    else
      let cid = assoc |> List.assoc "cid" |> to_string in
      let mime_type = assoc |> List.assoc "mimeType" |> to_string in
      Untyped {cid; mime_type}
  with e ->
    invalid_arg
      (Printf.sprintf "of_yojson: invalid blob ref (%s)" (Printexc.to_string e))

let to_yojson blob =
  let open Yojson.Safe.Util in
  let json =
    match blob.original with
    | Typed {type'; ref; mime_type; size} ->
        `Assoc
          [ ("$type", `String type')
          ; ("ref", `Assoc [("$link", `String (Cid.to_string ref))])
          ; ("mimeType", `String mime_type)
          ; ("size", `Int size) ]
    | Untyped {cid; mime_type} ->
        `Assoc [("cid", `String cid); ("mimeType", `String mime_type)]
  in
  json

let of_json_ref json =
  match json with
  | Typed {ref; mime_type; size; _} ->
      {original= json; ref; mime_type; size}
  | Untyped {cid; mime_type} ->
      { original= json
      ; ref= Result.get_ok @@ Cid.of_string cid
      ; mime_type
      ; size= 0 }

let to_ipld blob =
  StringMap.of_list
    [ ("$type", `String "blob")
    ; ("ref", `Link blob.ref)
    ; ("mimeType", `String blob.mime_type)
    ; ("size", `Integer blob.size) ]
