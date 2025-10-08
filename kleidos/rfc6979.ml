(* rfc 6979 nonce "k" generation *)

(* curve orders *)
let n_secp256k1 =
  Z.of_string_base 16
    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141"

let n_secp256r1 =
  Z.of_string_base 16
    "FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551"

(* 32-byte big-endian to Z *)
let bytes32_to_z (b : bytes) : Z.t =
  if Bytes.length b <> 32 then invalid_arg "expected 32 bytes" ;
  Bytes.fold_left
    (fun acc c -> Z.(add (shift_left acc 8) (of_int (Char.code c))))
    Z.zero b

(* Z to fixed 32-byte big-endian *)
let z_to_bytes32 (z : Z.t) : bytes =
  let out = Bytes.make 32 '\x00' in
  let rec fill i v =
    if i < 0 || v = Z.zero then ()
    else (
      Bytes.set out i (Char.chr Z.(to_int (logand v (of_int 0xFF)))) ;
      fill (i - 1) Z.(shift_right v 8) )
  in
  fill 31 z ;
  if z >= Z.(shift_left one 256) then invalid_arg "integer too large" ;
  out

(* bits2int for qbits=256 (leftmost 256 bits is whole 32 bytes here) *)
let bits2int_256 (bs : bytes) : Z.t =
  (* if bs > 32 bytes (not the case here), we'd truncate *)
  let len = Bytes.length bs in
  let take = if len <= 32 then len else 32 in
  let acc = ref Z.zero in
  for i = 0 to take - 1 do
    acc := Z.(add (shift_left !acc 8) (of_int (Char.code (Bytes.get bs i))))
  done ;
  if len > 32 then
    (* shift right extra bits if longer (not expected with SHA-256) *)
    let extra_bits = (len - 32) * 8 in
    Z.shift_right !acc extra_bits
  else !acc

(* bits2octets per RFC 6979 §2.3.2: reduce hash to scalar-sized octets *)
let bits2octets_256 ~q h1 =
  let z1 = bits2int_256 h1 in
  let z2 = Z.(z1 mod q) in
  z_to_bytes32 z2

(* returns 32-byte k for given order q *)
let rfc6979_k_256_bytes ~(q : Z.t) ~(privkey : bytes) ~(msg : bytes) : bytes =
  if Bytes.length privkey <> 32 then invalid_arg "privkey must be 32 bytes" ;
  let x = bytes32_to_z privkey in
  if x <= Z.zero || x >= q then invalid_arg "privkey scalar out of range" ;
  let hash = Hacl_star.Hacl.SHA2_256.hash in
  let h1 = hash msg in
  (* 32-byte SHA-256 digest *)
  let x_octets = privkey in
  (* already 32 bytes big-endian *)
  let h1_red = bits2octets_256 ~q h1 in
  let v = Bytes.make 32 '\x01' in
  let k = Bytes.make 32 '\x00' in
  let concat parts =
    let total = List.fold_left (fun a b -> a + Bytes.length b) 0 parts in
    let out = Bytes.create total in
    ignore
    @@ List.fold_left
         (fun off b ->
           Bytes.blit b 0 out off (Bytes.length b) ;
           off + Bytes.length b )
         0 parts ;
    out
  in
  let hmac k v =
    Digestif.SHA256.(hmac_bytes ~key:(Bytes.to_string k) v |> to_raw_string)
    |> Bytes.of_string
  in
  (* step: K = HMAC_K(V || 0x00 || x || h1); V = HMAC_K(V) *)
  let k = hmac k (concat [v; Bytes.of_string "\x00"; x_octets; h1_red]) in
  let v = hmac k v in
  (* step: K = HMAC_K(V || 0x01 || x || h1); V = HMAC_K(V) *)
  let k = hmac k (concat [v; Bytes.of_string "\x01"; x_octets; h1_red]) in
  let v = hmac k v in
  (* loop *)
  let rec loop k v =
    (* a. V = HMAC_K(V) *)
    let v = hmac k v in
    let t = v in
    let k_candidate = bits2int_256 t in
    if Z.(k_candidate >= one && k_candidate < q) then t
    else
      (* K = HMAC_K(V || 0x00); V = HMAC_K(V) *)
      let k = hmac k (concat [v; Bytes.of_string "\x00"]) in
      let v = hmac k v in
      loop k v
  in
  loop k v

let k_for_k256 ~(privkey : bytes) ~(msg : bytes) : bytes =
  rfc6979_k_256_bytes ~q:n_secp256k1 ~privkey ~msg

let k_for_p256 ~(privkey : bytes) ~(msg : bytes) : bytes =
  rfc6979_k_256_bytes ~q:n_secp256r1 ~privkey ~msg
