module String_map = Dag_cbor.String_map

type value =
  [ Dag_cbor.value
  | `BlobRef of Blob_ref.t
  | `LexArray of value Array.t
  | `LexMap of value String_map.t ]

let rec to_ipld (v : value) : Dag_cbor.value =
  match v with
  | `BlobRef r -> (
    match r.original with
    | Typed {ref; mime_type; size; _} ->
        `Map
          (String_map.of_list
             [ ("$type", `String "blob")
             ; ("ref", `Link ref)
             ; ("mimeType", `String mime_type)
             ; ("size", `Integer size) ] )
    | Untyped {cid; mime_type} ->
        `Map
          (String_map.of_list
             [("cid", `String cid); ("mimeType", `String mime_type)] ) )
  | `LexArray a ->
      `Array (Array.map to_ipld a)
  | `LexMap m ->
      `Map (String_map.map to_ipld m)
  | `Boolean b ->
      `Boolean b
  | `Integer i ->
      `Integer i
  | `Float f ->
      `Float f
  | `Bytes b ->
      `Bytes b
  | `String s ->
      `String s
  | `Array a ->
      `Array a
  | `Map m ->
      `Map m
  | `Link l ->
      `Link l
  | `Null ->
      `Null

let rec of_ipld (v : Dag_cbor.value) : value =
  match v with
  | `Map m ->
      if
        (String_map.mem "$type" m && String_map.find "$type" m = `String "blob")
        || (String_map.mem "cid" m && String_map.mem "mimeType" m)
      then `BlobRef (Blob_ref.of_ipld (`Map m))
      else `LexMap (String_map.map of_ipld m)
  | `Array a ->
      `LexArray (Array.map of_ipld a)
  | `Boolean b ->
      `Boolean b
  | `Integer i ->
      `Integer i
  | `Float f ->
      `Float f
  | `Bytes b ->
      `Bytes b
  | `String s ->
      `String s
  | `Link l ->
      `Link l
  | `Null ->
      `Null

let to_cbor_block obj =
  let ipld = to_ipld obj in
  let encoded = Dag_cbor.encode ipld in
  let cid = Cid.create Dcbor encoded in
  (cid, encoded)

let of_yojson (v : Yojson.Safe.t) : value = of_ipld (Dag_cbor.of_yojson v)

let to_yojson (v : value) : Yojson.Safe.t = Dag_cbor.to_yojson (to_ipld v)

type repo_record =
  (value String_map.t
  [@of_yojson
    fun v ->
      match of_yojson v with
      | `LexMap m ->
          Ok m
      | _ ->
          Error "decoded non-map value"]
  [@to_yojson fun v -> to_yojson (`LexMap v)] )
[@@deriving yojson]

let repo_record_to_string (record : repo_record) =
  record |> repo_record_to_yojson |> Yojson.Safe.to_string

let repo_record_to_cbor_block (record : repo_record) =
  to_cbor_block (`LexMap record)

let of_cbor encoded : repo_record =
  let decoded = Dag_cbor.decode encoded in
  match decoded with
  | `Map m ->
      String_map.map of_ipld m
  | _ ->
      raise (Failure "Decoded non-record value")
