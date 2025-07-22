open Util

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
  | Null ->
      write_uint_8 t 0xf6 (* null *)
  | Boolean b ->
      write_uint_8 t (if b then 0xf5 else 0xf4) (* true/false *)
  | Integer i ->
      write_integer t i
  | Float f ->
      write_float t f
  | Bytes b ->
      write_bytes t b
  | String s ->
      write_string t s
  | Array lst ->
      let len = List.length lst in
      write_type_and_argument t 4 (Int64.of_int len) ;
      List.iter (write_value t) lst
  | Map m ->
      if StringMap.mem "$link" m then
        match StringMap.find "$link" m with
        | Link cid ->
            write_cid t cid
        | _ ->
            invalid_arg "Object contains $link but value is not a cid-link"
      else if StringMap.mem "$bytes" m then
        match StringMap.find "$bytes" m with
        | Bytes b ->
            write_bytes t b
        | _ ->
            invalid_arg "Object contains $bytes but value is not bytes"
      else
        let len = StringMap.cardinal m in
        write_type_and_argument t 5 (Int64.of_int len) ;
        StringMap.iter (fun k v -> write_string t k ; write_value t v) m
  | Link cid ->
      write_cid t cid

let encode (v : value) : bytes =
  let encoder = create () in
  write_value encoder v ;
  if encoder.pos < Buffer.length encoder.buf then
    Buffer.truncate encoder.buf encoder.pos ;
  Buffer.to_bytes encoder.buf
