module StringMap = Map.Make (String)

let ordered_map_keys (m : 'a StringMap.t) : string list =
  let keys = StringMap.bindings m in
  List.map fst keys |> List.sort String.compare

let type_info_length len =
  if len < 24 then 1
  else if len < 0x100 then 2
  else if len < 0x10000 then 3
  else if len < 0x100000000 then 5
  else 9

type value =
  | Null
  | Boolean of bool
  | Integer of int64
  | Float of float
  | Bytes of bytes
  | String of string
  | Array of value list
  | Map of value StringMap.t
  | Link of Cid.t
