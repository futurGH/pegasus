let () =
  Mirage_crypto_rng_unix.use_default () ;
  let rotation_key =
    Kleidos.K256.(generate_keypair () |> fst |> privkey_to_multikey)
  in
  let jwt_key =
    Kleidos.K256.(generate_keypair () |> fst |> privkey_to_multikey)
  in
  let dpop_nonce_secret =
    Base64.(encode ~alphabet:uri_safe_alphabet ~pad:false)
      (Mirage_crypto_rng_unix.getrandom 32)
    |> Result.get_ok
  in
  Printf.printf
    {|PDS_ROTATION_KEY_MULTIBASE=%s
PDS_JWK_MULTIBASE=%s
PDS_DPOP_NONCE_SECRET=%s
|}
    rotation_key jwt_key dpop_nonce_secret
