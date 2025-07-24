module StringMap = Dag_cbor.StringMap

type typed_json_ref = {type': string; ref: Cid.t; mime_type: string; size: int64}

type untyped_json_ref = {cid: string; mime_type: string}

type json_ref = Typed of typed_json_ref | Untyped of untyped_json_ref

type t = {original: json_ref; ref: Cid.t; mime_type: string; size: int64}

let of_yojson json =
  let open Yojson.Safe.Util in
  let assoc = to_assoc json in
  try
    if List.mem_assoc "$type" assoc then
      let type' = assoc |> List.assoc "$type" |> to_string in
      if type' = "blob" then
        let ref = assoc |> List.assoc "ref" |> Cid.of_yojson |> Result.get_ok in
        let mime_type = assoc |> List.assoc "mimeType" |> to_string in
        let maybe_size = assoc |> List.assoc "size" in
        let size =
          match maybe_size with
          | `Int i ->
              Int64.of_int i
          | `Intlit i ->
              Int64.of_string i
          | _ ->
              0L
        in
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
  let json =
    match blob.original with
    | Typed {type'; ref; mime_type; size} ->
        `Assoc
          [ ("$type", `String type')
          ; ("ref", Cid.to_yojson ref)
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
      ; size= 0L }

let to_ipld blob : Dag_cbor.value StringMap.t =
  StringMap.of_list
    [ ("$type", `String "blob")
    ; ("ref", `Link blob.ref)
    ; ("mimeType", `String blob.mime_type)
    ; ("size", `Integer blob.size) ]

let of_ipld (ipld : Dag_cbor.value) =
  match ipld with
  | `Map m -> (
    try
      if StringMap.mem "$type" m then
        let type' =
          match StringMap.find "$type" m with
          | `String "blob" ->
              "blob"
          | _ ->
              invalid_arg "of_ipld: invalid blob ref $type"
        in
        let ref =
          match StringMap.find "ref" m with
          | `Link ref ->
              ref
          | _ ->
              invalid_arg "of_ipld: invalid blob ref ref"
        in
        let mime_type =
          match StringMap.find "mimeType" m with
          | `String mime_type ->
              mime_type
          | _ ->
              invalid_arg "of_ipld: invalid blob ref mimeType"
        in
        let size =
          match StringMap.find "size" m with
          | `Integer size ->
              size
          | _ ->
              invalid_arg "of_ipld: invalid blob ref size"
        in
        of_json_ref (Typed {type'; ref; mime_type; size})
      else if StringMap.mem "cid" m then
        let cid =
          match StringMap.find "cid" m with
          | `String cid ->
              cid
          | _ ->
              invalid_arg "of_ipld: invalid blob ref cid"
        in
        let mime_type =
          match StringMap.find "mimeType" m with
          | `String mime_type ->
              mime_type
          | _ ->
              invalid_arg "of_ipld: invalid blob ref mimeType"
        in
        of_json_ref (Untyped {cid; mime_type})
      else invalid_arg "of_ipld: invalid blob ref"
    with
    | Not_found ->
        invalid_arg "of_ipld: incomplete blob ref"
    | _ ->
        invalid_arg "of_ipld: invalid blob ref" )
  | _ ->
      invalid_arg "of_ipld: invalid blob ref"
