module K256 = struct
  open Hacl_star.Hacl

  let sign ~privkey ~msg : bytes =
    let hashed = SHA2_256.hash msg in
    let k = Rfc6979.k_for_k256 ~privkey ~msg in
    match K256.Libsecp256k1.sign ~sk:privkey ~msg:hashed ~k with
    | Some signature ->
        signature
    | None ->
        failwith "failed to sign message"

  let verify ~pubkey ~msg ~signature : bool =
    let hashed = SHA2_256.hash msg in
    K256.Libsecp256k1.verify ~pk:pubkey ~msg:hashed ~signature
end

module P256 = struct
  open Hacl_star.Hacl

  let sign ~privkey ~msg : bytes =
    let hashed = SHA2_256.hash msg in
    let k = Rfc6979.k_for_p256 ~privkey ~msg in
    match P256.sign ~sk:privkey ~msg:hashed ~k with
    | Some signature ->
        signature
    | None ->
        failwith "failed to sign message"

  let verify ~pubkey ~msg ~signature : bool =
    let hashed = SHA2_256.hash msg in
    P256.verify ~pk:pubkey ~msg:hashed ~signature
end
