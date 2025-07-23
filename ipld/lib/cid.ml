type t =
  { (* We only implement CIDv1 *)
    version: int
  ; (* Multicodec type for the data; 0x55 for raw data or 0x71 for DAG-CBOR *)
    codec: codec
  ; (* Digest of the data *)
    digest: digest
  ; (* CID bytes *)
    bytes: bytes }

and digest =
  { (* Multicodec type for the digest; always 0x12 for SHA-256 *)
    codec: int
  ; (* Hash bytes *)
    contents: bytes }

and codec = Raw | Dcbor

let version = 1

let hash_sha256 = 0x12

let codec_raw = 0x55

let codec_dcbor = 0x71

let codec_byte = function Raw -> codec_raw | Dcbor -> codec_dcbor

let codec_of_byte b = if b = codec_raw then Raw else Dcbor

let create codec data =
  let buf = Buffer.create 36 in
  Buffer.add_uint8 buf version ;
  Buffer.add_uint8 buf @@ codec_byte codec ;
  Buffer.add_uint8 buf hash_sha256 ;
  Buffer.add_uint8 buf 32 ;
  let digest = Digestif.SHA256.(data |> digest_bytes |> to_raw_string) in
  Buffer.add_string buf digest ;
  let bytes = Buffer.to_bytes buf in
  { version
  ; codec
  ; digest= {codec= hash_sha256; contents= Bytes.of_string digest}
  ; bytes }

let create_empty codec =
  let buf = Buffer.create 4 in
  Buffer.add_uint8 buf version ;
  Buffer.add_uint8 buf @@ codec_byte codec ;
  Buffer.add_uint8 buf hash_sha256 ;
  Buffer.add_uint8 buf 0 ;
  let bytes = Buffer.to_bytes buf in
  { version
  ; codec
  ; digest= {codec= hash_sha256; contents= Bytes.sub bytes 4 0}
  ; bytes }

let decode_first bytes =
  let version = Char.code (Bytes.get bytes 0) in
  let codec = Char.code (Bytes.get bytes 1) in
  let digest_codec = Char.code (Bytes.get bytes 2) in
  let digest_length = Char.code (Bytes.get bytes 3) in
  if version <> 1 then
    failwith (Printf.sprintf "Unsupported CID version %d" version) ;
  if codec <> codec_raw && codec <> codec_dcbor then
    failwith (Printf.sprintf "Unsupported CID codec %d" codec) ;
  if digest_codec <> hash_sha256 then
    failwith (Printf.sprintf "Unsupported CID digest codec %d" digest_codec) ;
  if digest_length <> 32 && digest_length <> 0 then
    failwith (Printf.sprintf "Incorrect CID digest length %d" digest_length) ;
  if Bytes.length bytes < 4 + digest_length then
    failwith (Printf.sprintf "CID too short %d" (Bytes.length bytes)) ;
  ( { version
    ; codec= codec_of_byte codec
    ; digest= {codec= digest_codec; contents= Bytes.sub bytes 4 digest_length}
    ; bytes= Bytes.sub bytes 0 (digest_length + 4) }
  , Bytes.sub bytes (digest_length + 4) (Bytes.length bytes - digest_length - 4)
  )

let decode bytes =
  let cid, remainder = decode_first bytes in
  if Bytes.length remainder > 0 then
    failwith
      (Printf.sprintf "CID has %d trailing bytes" (Bytes.length remainder)) ;
  cid

let of_string str =
  (* 36 byte CID in base32 = 58 chars + 1 char prefix *)
  (* 4 byte CID in base32 = 7 chars + 1 char prefix *)
  if String.length str <> 59 && String.length str <> 8 then
    Error (Printf.sprintf "CID too short %s" str)
  else
    match Multibase.decode str with
    | Ok (_, cid) -> (
      match decode (Bytes.of_string cid) with
      | cid ->
          Ok cid
      | exception msg ->
          Error (Printf.sprintf "CID decode error: %s" (Printexc.to_string msg))
      )
    | Error (`Msg msg) ->
        Error msg
    | Error (`Unsupported t) ->
        Error
          (Printf.sprintf "Unsupported multibase %s"
             (Multibase.Encoding.to_string t) )

let to_string cid =
  match Multibase.encode `Base32 (Bytes.to_string cid.bytes) with
  | Ok str ->
      str
  | Error (`Msg msg) ->
      failwith msg
  | Error (`Unsupported t) ->
      failwith
        (Printf.sprintf "Unsupported multibase %s"
           (Multibase.Encoding.to_string t) )

let of_bytes bytes =
  (* 36 byte CID + 1 byte prefix *)
  (* 4 byte CID + 1 byte prefix *)
  if Bytes.length bytes <> 37 && Bytes.length bytes <> 5 then
    Error (Printf.sprintf "CID too short %d" (Bytes.length bytes))
  else if Bytes.get bytes 0 <> Char.unsafe_chr 0 then
    Error (Printf.sprintf "CID has non-zero prefix %c" (Bytes.get bytes 0))
  else Ok (decode (Bytes.sub bytes 1 (Bytes.length bytes - 1)))

let to_bytes cid =
  let buf = Buffer.create (1 + Bytes.length cid.bytes) in
  Buffer.add_uint8 buf 0 ;
  Buffer.add_bytes buf cid.bytes ;
  Buffer.to_bytes buf

let as_cid str = Result.get_ok @@ of_string str

let compare a b = String.compare (to_string a) (to_string b)

let equal a b = String.equal (to_string a) (to_string b)

let hash = Hashtbl.hash
