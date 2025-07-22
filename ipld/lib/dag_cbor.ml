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
  [ `Null
  | `Boolean of bool
  | `Integer of int64
  | `Float of float
  | `Bytes of bytes
  | `String of string
  | `Array of value list
  | `Map of value StringMap.t
  | `Link of Cid.t]


module Encoder = struct
type t = {mutable buf: Buffer.t; mutable pos: int}

let create () = {buf= Buffer.create 1024; pos= 0}

let write_float_64 t f =
  let i64 = Int64.bits_of_float f in
  let bytes = Bytes.create 8 in
  Bytes.set_int64_be bytes 0 i64 ;
  Buffer.add_bytes t.buf bytes ;
  t.pos <- t.pos + 8

let write_uint_8 t i =
  if i < 0 || i > 255 then
    invalid_arg "write_uint_8: value out of range (0-255)" ;
  Buffer.add_uint8 t.buf i ;
  t.pos <- t.pos + 1

let write_uint_16 t i =
  if i < 0 || i > 65535 then
    invalid_arg "write_uint_16: value out of range (0-65535)" ;
  Buffer.add_uint16_be t.buf i ;
  t.pos <- t.pos + 2

let write_uint_32 t (i : int32) =
  Buffer.add_int32_be t.buf i ;
  t.pos <- t.pos + 4

let write_uint_53 t (i : int64) =
  if i < 0L || i > 9007199254740991L then
    invalid_arg "write_uint_53: value out of range (0-9007199254740991)" ;
  Buffer.add_int64_be t.buf i ;
  t.pos <- t.pos + 8

let write_type_and_argument t major (arg : int64) =
  let type_code =
    match major with
    | 0 ->
        0x00 (* unsigned integer *)
    | 1 ->
        0x20 (* negative integer *)
    | 2 ->
        0x40 (* byte string *)
    | 3 ->
        0x60 (* text string *)
    | 4 ->
        0x80 (* array *)
    | 5 ->
        0xa0 (* map *)
    | 6 ->
        0xc0 (* tag *)
    | _ ->
        invalid_arg "write_type_and_argument: invalid major type"
  in
  if arg < 24L then write_uint_8 t (type_code lor Int64.to_int arg)
  else if arg < 0x100L then (
    write_uint_8 t (type_code lor 24) ;
    write_uint_8 t (Int64.to_int arg) )
  else if arg < 0x10000L then (
    write_uint_8 t (type_code lor 25) ;
    write_uint_16 t (Int64.to_int arg) )
  else if arg < 0x100000000L then (
    write_uint_8 t (type_code lor 26) ;
    write_uint_32 t (Int64.to_int32 arg) )
  else (
    write_uint_8 t (type_code lor 27) ;
    write_uint_53 t arg )

let write_integer t (i : int64) =
  if i >= 0L then write_type_and_argument t 0 i
  else write_type_and_argument t 1 (Int64.sub (Int64.neg i) 1L)

let write_float t (f : float) = write_uint_8 t 0xfb ; write_float_64 t f

let write_string t (s : string) =
  let len = String.length s in
  write_type_and_argument t 3 (Int64.of_int len) ;
  Buffer.add_string t.buf s ;
  t.pos <- t.pos + len

let write_bytes t (b : bytes) =
  let len = Bytes.length b in
  write_type_and_argument t 2 (Int64.of_int len) ;
  Buffer.add_bytes t.buf b ;
  t.pos <- t.pos + len

let write_cid t (cid : Cid.t) =
  let cid_bytes = Cid.to_cstruct cid in
  let bytes_len = Cstruct.length cid_bytes + 1 in
  write_type_and_argument t 6 42L ;
  write_type_and_argument t 2 (Int64.of_int bytes_len) ;
  (* CID bytes are prefixed with the 0x00 multibase identity prefix *)
  Buffer.add_uint8 t.buf 0 ;
  Buffer.add_bytes t.buf (Cstruct.to_bytes cid_bytes) ;
  t.pos <- t.pos + bytes_len

let rec write_value t (v : value) =
  match v with
  | `Null ->
      write_uint_8 t 0xf6 (* null *)
  | `Boolean b ->
      write_uint_8 t (if b then 0xf5 else 0xf4) (* true/false *)
  | `Integer i ->
      write_integer t i
  | `Float f ->
      write_float t f
  | `Bytes b ->
      write_bytes t b
  | `String s ->
      write_string t s
  | `Array lst ->
      let len = List.length lst in
      write_type_and_argument t 4 (Int64.of_int len) ;
      List.iter (write_value t) lst
  | `Map m ->
      if StringMap.mem "$link" m then
        match StringMap.find "$link" m with
        | `Link cid ->
            write_cid t cid
        | _ ->
            invalid_arg "Object contains $link but value is not a cid-link"
      else if StringMap.mem "$bytes" m then
        match StringMap.find "$bytes" m with
        | `Bytes b ->
            write_bytes t b
        | _ ->
            invalid_arg "Object contains $bytes but value is not bytes"
      else
        let len = StringMap.cardinal m in
        write_type_and_argument t 5 (Int64.of_int len) ;
        StringMap.iter (fun k v -> write_string t k ; write_value t v) m
  | `Link cid ->
      write_cid t cid

let encode (v : value) : bytes =
  let encoder = create () in
  write_value encoder v ;
  if encoder.pos < Buffer.length encoder.buf then
    Buffer.truncate encoder.buf encoder.pos ;
  Buffer.to_bytes encoder.buf
end

module Decoder = struct
type t = {mutable buf: bytes; mutable pos: int}

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
end
