module StringMap = Dag_cbor.StringMap

type value =
  [ Dag_cbor.value
  | `BlobRef of Blob_ref.t
  | `LexArray of value Array.t
  | `LexMap of value StringMap.t ]

type repo_record = value StringMap.t

let rec to_ipld (v : value) : Dag_cbor.value =
  match v with
  | `BlobRef r -> (
    match r.original with
    | Typed {ref; mime_type; size; _} ->
        `Map
          (StringMap.of_list
             [ ("$type", `String "blob")
             ; ("ref", `Link ref)
             ; ("mimeType", `String mime_type)
             ; ("size", `Integer size) ] )
    | Untyped {cid; mime_type} ->
        `Map
          (StringMap.of_list
             [("cid", `String cid); ("mimeType", `String mime_type)] ) )
  | `LexArray a ->
      `Array (Array.map to_ipld a)
  | `LexMap m ->
      `Map (StringMap.map to_ipld m)
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
        StringMap.mem "$type" m
        || (StringMap.mem "cid" m && StringMap.mem "mimeType" m)
        || (StringMap.mem "size" m && StringMap.mem "mimeType" m)
      then `BlobRef (Blob_ref.of_ipld (`Map m))
      else `LexMap (StringMap.map of_ipld m)
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

let of_cbor encoded : repo_record =
  let decoded = Dag_cbor.decode encoded in
  match of_ipld decoded with
  | `LexMap m ->
      m
  | _ ->
      raise (Failure "Decoded non-record value")

let of_yojson (v : Yojson.Safe.t) : value = of_ipld (Dag_cbor.of_yojson v)

let to_yojson (v : value) : Yojson.Safe.t = Dag_cbor.to_yojson (to_ipld v)
