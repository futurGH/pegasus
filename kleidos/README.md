# kleidos

is an atproto-valid interface for secp256k1 and secp256r1 (P-256) key management, signing, verification, and encoding.

The library provides a unified interface for working with both elliptic curves used in atproto, with support for multikey encoding and did:key generation.

## installation

Add to your `dune-project`:

```lisp
(depends
  kleidos)
```

## usage

Both K-256 and P-256 share the same interface through the `CURVE` module type.

### generating keys

```ocaml
open Kleidos

(* Generate a K-256 keypair *)
let (privkey, pubkey) = K256.generate_keypair ()

(* Generate a P-256 keypair *)
let (privkey, pubkey) = P256.generate_keypair ()
```

### signing and verifying

```ocaml
(* Sign a message *)
let msg = Bytes.of_string "Hello, atproto!" in
let signature = K256.sign ~privkey ~msg

(* Verify a signature *)
let is_valid = K256.verify ~pubkey ~msg ~signature
(* => true *)
```

### key encoding

```ocaml
(* Convert keys to multikey format *)
let privkey_multikey = K256.privkey_to_multikey privkey
(* => "z2MkApQ..." *)

let pubkey_multikey = K256.pubkey_to_multikey pubkey
(* => "zQ3sh..." *)

(* Generate a DID key *)
let did_key = K256.pubkey_to_did_key pubkey
(* => "did:key:zQ3sh..." *)
```

### deriving public keys

```ocaml
(* Derive public key from private key *)
let pubkey = K256.derive_pubkey ~privkey
```

### key validation

```ocaml
(* Check if a private key is valid *)
let is_valid = K256.is_valid_privkey privkey
```

## implementation details

- Implements [RFC 6979](https://datatracker.ietf.org/doc/html/rfc6979) for deterministic signature generation
- Implements low-S normalization to prevent signature malleability
- All signatures are deterministic given the same private key and message
