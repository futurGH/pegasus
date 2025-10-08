open struct
  let to_multikey key ~prefix : string =
    match
      Multibase.encode_t `Base58btc (Bytes.to_string @@ Bytes.cat prefix key)
    with
    | Ok multikey ->
        multikey
    | Error (`Msg msg) ->
        failwith (Format.sprintf "failed to encode key as multikey: %s" msg)

  let bytes_of_multikey multikey : bytes =
    match Multibase.decode multikey with
    | Ok (_, k) ->
        Bytes.of_string k
    | Error (`Msg msg) ->
        failwith msg
    | Error (`Unsupported e) ->
        failwith
          ( "unsupported key multibase encoding "
          ^ Multibase.Encoding.to_string e )
end

module type CURVE = sig
  val name : string

  val public_prefix : bytes

  val private_prefix : bytes

  val normalize_pubkey_to_raw : bytes -> bytes

  val sign : privkey:bytes -> msg:bytes -> bytes

  val verify : pubkey:bytes -> msg:bytes -> signature:bytes -> bool

  val is_valid_privkey : bytes -> bool

  val derive_pubkey : privkey:bytes -> bytes

  val generate_keypair : unit -> bytes * bytes

  val privkey_to_multikey : bytes -> string

  val pubkey_to_multikey : bytes -> string

  val pubkey_to_did_key : bytes -> string
end

module K256 : CURVE = struct
  open Hacl_star.Hacl

  let name = "K256"

  let public_prefix = Bytes.of_string "\xe7\x01"

  let private_prefix = Bytes.of_string "\x81\x26"

  let normalize_pubkey_to_raw key : bytes =
    match Bytes.length key with
    | 64 | 32 ->
        key
    | 65 -> (
      match K256.uncompressed_to_raw key with
      | Some raw ->
          raw
      | None ->
          failwith "invalid uncompressed key" )
    | 33 -> (
      match K256.compressed_to_raw key with
      | Some raw ->
          raw
      | None ->
          failwith "invalid compressed key" )
    | len ->
        failwith ("invalid key length: " ^ string_of_int len)

  let sign ~privkey ~msg : bytes =
    let hashed = SHA2_256.hash msg in
    let k = Rfc6979.k_for_k256 ~privkey ~msg in
    match K256.Libsecp256k1.sign ~sk:privkey ~msg:hashed ~k with
    | Some sgn ->
        Low_s.normalize_k256 sgn
    | None ->
        failwith "failed to sign message"

  let verify ~pubkey ~msg ~signature : bool =
    let hashed = SHA2_256.hash msg in
    let pk = normalize_pubkey_to_raw pubkey in
    K256.Libsecp256k1.verify ~pk ~msg:hashed ~signature

  let is_valid_privkey privkey : bool = K256.valid_sk ~sk:privkey

  let derive_pubkey ~privkey : bytes =
    if not (is_valid_privkey privkey) then failwith "invalid p256 private key" ;
    match K256.secret_to_public ~sk:privkey with
    | Some pubkey ->
        K256.raw_to_compressed pubkey
    | None ->
        failwith "failed to derive public key"

  let generate_keypair () : bytes * bytes =
    (* P256 is fine for generating a privkey for either curve,
       but the accompanying public key won't work K256 *)
    let open Mirage_crypto_ec.P256.Dsa in
    let privkey = generate () |> fst |> priv_to_octets |> Bytes.of_string in
    (privkey, derive_pubkey ~privkey)

  let privkey_to_multikey privkey : string =
    to_multikey privkey ~prefix:private_prefix

  let pubkey_to_multikey pubkey : string =
    to_multikey pubkey ~prefix:public_prefix

  let pubkey_to_did_key pubkey : string = "did:key:" ^ pubkey_to_multikey pubkey
end

module P256 : CURVE = struct
  open Hacl_star.Hacl

  let name = "P256"

  let public_prefix = Bytes.of_string "\x80\x24"

  let private_prefix = Bytes.of_string "\x86\x26"

  let normalize_pubkey_to_raw key : bytes =
    match Bytes.length key with
    | 64 | 32 ->
        key
    | 65 -> (
      match P256.uncompressed_to_raw key with
      | Some raw ->
          raw
      | None ->
          failwith "invalid uncompressed key" )
    | 33 -> (
      match P256.compressed_to_raw key with
      | Some raw ->
          raw
      | None ->
          failwith "invalid compressed key" )
    | len ->
        failwith ("invalid key length: " ^ string_of_int len)

  let sign ~privkey ~msg : bytes =
    let hashed = SHA2_256.hash msg in
    let k = Rfc6979.k_for_p256 ~privkey ~msg in
    match P256.sign ~sk:privkey ~msg:hashed ~k with
    | Some sgn ->
        Low_s.normalize_p256 sgn
    | None ->
        failwith "failed to sign message"

  let verify ~pubkey ~msg ~signature : bool =
    let hashed = SHA2_256.hash msg in
    let pk = normalize_pubkey_to_raw pubkey in
    P256.verify ~pk ~msg:hashed ~signature

  let is_valid_privkey privkey : bool = P256.valid_sk ~sk:privkey

  let derive_pubkey ~privkey : bytes =
    if not (is_valid_privkey privkey) then failwith "invalid p256 private key" ;
    match P256.dh_initiator ~sk:privkey with
    | Some pubkey ->
        P256.raw_to_compressed pubkey
    | None ->
        failwith "failed to derive public key"

  let generate_keypair () : bytes * bytes =
    (* don't know why but the pubkey returned by generate () fails to validate
       so we derive our own *)
    let open Mirage_crypto_ec.P256.Dsa in
    let privkey = generate () |> fst |> priv_to_octets |> Bytes.of_string in
    (privkey, derive_pubkey ~privkey)

  let privkey_to_multikey privkey : string =
    to_multikey privkey ~prefix:private_prefix

  let pubkey_to_multikey pubkey : string =
    to_multikey pubkey ~prefix:public_prefix

  let pubkey_to_did_key pubkey : string = "did:key:" ^ pubkey_to_multikey pubkey
end

type key = bytes * (module CURVE)

let parse_multikey_bytes bytes : key =
  if Bytes.length bytes < 3 then failwith "multikey too short" ;
  let b0 = int_of_char (Bytes.get bytes 0) in
  let b1 = int_of_char (Bytes.get bytes 1) in
  let type_code = (b0 lsl 8) lor b1 in
  let key = Bytes.sub bytes 2 (Bytes.length bytes - 2) in
  match type_code with
  | 0x8626 ->
      (* p256 privkey *)
      (key, (module P256 : CURVE))
  | 0x8024 ->
      (* p256 pubkey *)
      (key, (module P256 : CURVE))
  | 0x8126 ->
      (* k256 privkey *)
      (key, (module K256 : CURVE))
  | 0xe701 ->
      (* k256 pubkey *)
      (key, (module K256 : CURVE))
  | _ ->
      failwith (Printf.sprintf "invalid key type 0x%04x" type_code)

let parse_multikey_str multikey : key =
  multikey |> bytes_of_multikey |> parse_multikey_bytes

let sign ~privkey ~msg : bytes =
  let privkey, (module Curve : CURVE) = privkey in
  Curve.sign ~privkey ~msg

let pubkey_to_did_key pubkey : string =
  let pubkey, (module Curve : CURVE) = pubkey in
  Curve.pubkey_to_did_key pubkey
