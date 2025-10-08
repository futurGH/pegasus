open struct
  let z_of_bytes s =
    let acc = ref Z.zero in
    for i = 0 to String.length s - 1 do
      acc := Z.(shift_left !acc 8 + of_int (Char.code s.[i]))
    done ;
    !acc

  let bytes_of_z_32 z =
    let buf = Bytes.make 32 '\x00' in
    let rec fill i v =
      if i >= 0 then (
        Bytes.set buf i (Char.chr (Z.to_int (Z.logand v (Z.of_int 0xff)))) ;
        fill (i - 1) (Z.shift_right v 8) )
    in
    fill 31 z ; Bytes.unsafe_to_string buf

  let to_32 s =
    let l = String.length s in
    if l = 32 then s
    else if l < 32 then String.make (32 - l) '\x00' ^ s
    else String.sub s (l - 32) 32
end

let normalize ~(order : Z.t) (r : string) (s : string) : bytes =
  let r32 = to_32 r in
  let s32 = to_32 s in
  let s_z = z_of_bytes s32 in
  let s_low = if Z.gt s_z Z.(order / ~$2) then Z.(order - s_z) else s_z in
  r32 ^ bytes_of_z_32 s_low |> Bytes.of_string

let k256_n =
  Z.of_string
    "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141"

let p256_n =
  Z.of_string
    "0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551"

let rs_of_sig (signature : bytes) : string * string =
  let sig_str = Bytes.to_string signature in
  let sig_len = String.length sig_str in
  if sig_len <> 64 then
    invalid_arg
      (Format.sprintf "signature must be 64 bytes (r||s); got %d" sig_len) ;
  let r = String.sub sig_str 0 32 in
  let s = String.sub sig_str 32 32 in
  (r, s)

let normalize_k256 sgn =
  let r, s = rs_of_sig sgn in
  normalize ~order:k256_n r s

let normalize_p256 sgn =
  let r, s = rs_of_sig sgn in
  normalize ~order:p256_n r s
