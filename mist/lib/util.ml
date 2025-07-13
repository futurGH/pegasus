let leading_zeros_on_hash (key : string) : int =
  let digest : string = Sha256.string key |> Sha256.to_bin in
  let rec loop idx zeros =
    if idx >= String.length digest then zeros
    else
      let byte = Char.code digest.[idx] in
      (* counting in 2-bit chunks, each byte can contain up to 4x 2-bit leading zeros *)
      let zeros' =
        zeros
        +
        if byte = 0x0 then 4
        else if byte < 0x04 then 3
        else if byte < 0x10 then 2
        else if byte < 0x40 then 1
        else 0
      in
      if byte = 0x0 then loop (idx + 1) zeros' else zeros'
  in
  loop 0 0

let shared_prefix_length (a : string) (b : string) : int =
  let rec loop idx =
    if idx >= String.length a || idx >= String.length b then idx
    else if a.[idx] = b.[idx] then loop (idx + 1)
    else idx
  in
  loop 0

let valid_key_char_regex = Str.regexp "^[a-zA-Z0-9_~\\-:.]*$"

let is_valid_mst_key (key : string) : bool =
  let parts = String.split_on_char '/' key in
  if String.length key > 1024 then false
  else
    match parts with
    | [part1; part2] ->
        if String.length part1 = 0 || String.length part2 = 0 then false
        else if not (Str.string_match valid_key_char_regex part1 0) then false
        else if not (Str.string_match valid_key_char_regex part2 0) then false
        else true
    | _ ->
        false
