open Util

type state = {mutable buf: bytes; mutable pos: int}

let read_float_64 t =
  if t.pos + 8 > Bytes.length t.buf then
    invalid_arg "read_float_64: not enough bytes in buffer" ;
  let bytes = Bytes.sub t.buf t.pos 8 in
  let f = Int64.float_of_bits (Bytes.get_int64_be bytes 0) in
  t.pos <- t.pos + 8 ;
  f

let read_uint_8 t =
  if t.pos + 1 > Bytes.length t.buf then
    invalid_arg "read_uint_8: not enough bytes in buffer" ;
  let i = Bytes.get_uint8 t.buf t.pos in
  t.pos <- t.pos + 1 ;
  i

let read_uint_16 t =
  if t.pos + 2 > Bytes.length t.buf then
    invalid_arg "read_uint_16: not enough bytes in buffer" ;
  let i = Bytes.get_uint16_be t.buf t.pos in
  t.pos <- t.pos + 2 ;
  i

let read_uint_32 t =
  if t.pos + 4 > Bytes.length t.buf then
    invalid_arg "read_uint_32: not enough bytes in buffer" ;
  let i = Bytes.get_int32_be t.buf t.pos in
  t.pos <- t.pos + 4 ;
  i

let read_uint_53 t =
  if t.pos + 8 > Bytes.length t.buf then
    invalid_arg "read_uint_53: not enough bytes in buffer" ;
  let i = Bytes.get_int64_be t.buf t.pos in
  t.pos <- t.pos + 8 ;
  if i < 0L || i > 9007199254740991L then
    invalid_arg "read_uint_53: value out of range (0-9007199254740991)" ;
  i

let read_argument t info =
  if info < 24L then info
  else
    let len : int64 =
      match info with
      | 24L ->
          Int64.of_int (read_uint_8 t)
      | 25L ->
          Int64.of_int (read_uint_16 t)
      | 26L ->
          Int64.of_int32 (read_uint_32 t)
      | 27L ->
          read_uint_53 t
      | _ ->
          invalid_arg "read_argument: invalid info value"
    in
    if len < 0L then invalid_arg "read_argument: negative length" ;
    len

let read_string t len =
  if t.pos + len > Bytes.length t.buf then
    invalid_arg "read_string: not enough bytes in buffer" ;
  let str = Bytes.sub_string t.buf t.pos len in
  t.pos <- t.pos + len ;
  str

let read_bytes t len =
  if t.pos + len > Bytes.length t.buf then
    invalid_arg "read_bytes: not enough bytes in buffer" ;
  let bytes = Bytes.sub t.buf t.pos len in
  t.pos <- t.pos + len ;
  bytes

let read_cid t len =
  if t.pos + len > Bytes.length t.buf then
    invalid_arg "read_cid: not enough bytes in buffer" ;
  let cid_bytes = Bytes.sub t.buf t.pos len in
  t.pos <- t.pos + len ;
  match Cid.of_cstruct ~base:`Base32 (Cstruct.of_bytes cid_bytes) with
  | Ok cid ->
      cid
  | Error (`Msg msg) ->
      failwith ("CID parse error: " ^ msg)
  | Error (`Unsupported _) ->
      failwith "CID parse error: unsupported encoding"

let decode_string_key t =
  let prelude = read_uint_8 t in
  let type_code = prelude lsr 5 in
  if type_code <> 3 then
    invalid_arg
      ( "decode_string_key: expected text string type; got "
      ^ string_of_int type_code ) ;
  let info = Int64.of_int (prelude land 0x1f) in
  let len = read_argument t info in
  if len < 0L then invalid_arg "decode_string_key: negative length" ;
  read_string t (Int64.to_int len)

let decode_first buf =
  let t = {buf; pos= 0} in
  let rec decode_first' () =
    if t.pos >= Bytes.length t.buf then
      invalid_arg "decode_first: no more bytes to decode" ;
    let prelude = read_uint_8 t in
    let major_type = prelude lsr 5 in
    let info = Int64.of_int (prelude land 0x1f) in
    match major_type with
    | 0 ->
        (* unsigned integer *)
        let value = read_argument t info in
        if value < 0L then invalid_arg "decode_first: negative unsigned integer" ;
        `Integer value
    | 1 ->
        (* negative integer *)
        let value = read_argument t info in
        if value < 0L then invalid_arg "decode_first: negative negative integer" ;
        `Integer (Int64.neg (Int64.add value 1L))
    | 2 ->
        (* byte string *)
        let len = read_argument t info in
        if len < 0L then invalid_arg "decode_first: negative byte string length" ;
        `Bytes (read_bytes t (Int64.to_int len))
    | 3 ->
        (* text string *)
        let len = read_argument t info in
        if len < 0L then invalid_arg "decode_first: negative text string length" ;
        `String (read_string t (Int64.to_int len))
    | 4 ->
        (* array *)
        let len = read_argument t info in
        if len < 0L then invalid_arg "decode_first: negative array length" ;
        let rec decode_array acc n =
          if n <= 0 then List.rev acc
          else
            let item = decode_first' () in
            decode_array (item :: acc) (n - 1)
        in
        `Array (decode_array [] (Int64.to_int len))
    | 5 ->
        (* map *)
        let len = read_argument t info in
        if len < 0L then invalid_arg "decode_first: negative map length" ;
        let rec decode_map acc n =
          if n <= 0 then StringMap.of_seq (List.to_seq acc)
          else
            let key = decode_string_key t in
            let value = decode_first' () in
            decode_map ((key, value) :: acc) (n - 1)
        in
        `Map (decode_map [] (Int64.to_int len))
    | 6 ->
        (* tag *)
        let tag = read_uint_8 t in
        if tag = 42 then (
          let prelude = read_uint_8 t in
          let type_code = prelude lsr 5 in
          if type_code <> 2 then
            invalid_arg
              ( "decode_first: expected type 2 for CID; got "
              ^ string_of_int type_code ) ;
          let info = Int64.of_int (prelude land 0x1f) in
          let len = read_argument t info in
          if len < 0L then invalid_arg "decode_first: negative CID length" ;
          let cid = read_cid t (Int64.to_int len) in
          `Link cid )
        else invalid_arg ("decode_first: unsupported tag " ^ string_of_int tag)
    | 7 -> (
      (* boolean, null, or float *)
      match info with
      | 20L | 21L ->
          `Boolean (info = 21L) (* true/false *)
      | 22L ->
          `Null
      | 27L ->
          `Float (read_float_64 t)
      | _ ->
          invalid_arg
            ( "decode_first: unsupported info value for major type 7: "
            ^ Int64.to_string info ) )
    | _ ->
        invalid_arg
          ("decode_first: unsupported major type " ^ string_of_int major_type)
  in
  let remainder = Bytes.sub t.buf t.pos (Bytes.length t.buf - t.pos) in
  let result : value = decode_first' () in
  (result, remainder)

let decode buf =
  let value, remainder = decode_first buf in
  if Bytes.length remainder > 0 then
    invalid_arg "decode: extra bytes after valid CBOR data" ;
  value
