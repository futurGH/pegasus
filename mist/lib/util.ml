let leading_zeros_on_hash (key : string) : int =
  let digest = Digestif.SHA256.(digest_string key |> to_raw_string) in
  let rec loop idx zeros =
    if idx >= String.length digest then zeros
    else
      let byte = Char.code digest.[idx] in
      (* counting in 2-bit chunks, each byte can contain up to 4x 2-bit leading zeros *)
      let zeros' =
        zeros
        +
        if byte = 0 then 4
        else if byte < 4 then 3
        else if byte < 16 then 2
        else if byte < 64 then 1
        else 0
      in
      if byte = 0 then loop (idx + 1) zeros' else zeros'
  in
  loop 0 0

let shared_prefix_length (a : string) (b : string) : int =
  let rec loop idx =
    if idx >= String.length a || idx >= String.length b then idx
    else if a.[idx] = b.[idx] then loop (idx + 1)
    else idx
  in
  loop 0

let shared_prefix (a : string) (b : string) : string =
  let len = shared_prefix_length a b in
  String.sub a 0 len

let valid_key_char_regex = Re.Pcre.regexp "^[a-zA-Z0-9_~\\-:.]*$"

let is_valid_mst_key (key : string) : bool =
  match String.split_on_char '/' key with
  | [coll; rkey]
    when String.length key <= 1024
         && coll <> "" && rkey <> ""
         && Re.execp valid_key_char_regex coll
         && Re.execp valid_key_char_regex rkey ->
      true
  | _ ->
      false

let ensure_valid_key (key : string) : unit =
  if not (is_valid_mst_key key) then raise (Invalid_argument "invalid mst key")
