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
  match String.split_on_char '/' key with
  | [coll; rkey]
    when String.length key <= 1024
         && coll <> "" && rkey <> ""
         && Str.string_match valid_key_char_regex coll 0
         && Str.string_match valid_key_char_regex rkey 0 ->
      true
  | _ ->
      false

let ensure_valid_key (key : string) : unit =
  if not (is_valid_mst_key key) then raise (Invalid_argument "Invalid MST key")

let encode_cbor_block (data : string) : bytes =
  let cbor_data = Cbor.encode_string data in
  let cbor_length = Bytes.length cbor_data in
  let length_bytes = Bytes.create 4 in
  Bytes.set_int32_le length_bytes 0 (Int32.of_int cbor_length) ;
  Bytes.cat length_bytes cbor_data
