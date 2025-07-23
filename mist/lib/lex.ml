type value =
  [ Dag_cbor.value
  | `BlobRef of string
  | `LexArray of value Array.t
  | `LexMap of value Dag_cbor.StringMap.t ]

type repo_record = value Dag_cbor.StringMap.t
