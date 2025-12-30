# ipld

is a mostly [DASL-compliant](https://dasl.ing/) implementation of [CIDs](https://dasl.ing/cid.html), [CAR](https://dasl.ing/car.html), and [DAG-CBOR](https://dasl.ing/drisl.html) for OCaml.

This library implements the core IPLD primitives needed for atproto.

## components

- **CID** - Content Identifiers (CIDv1) with SHA-256 digests
- **CAR** - Content Addressable aRchives for storing and transferring IPLD data
- **DAG-CBOR** - Deterministic CBOR encoding for content-addressed data

## installation

Add to your `dune-project`:

```lisp
(depends
  ipld)
```

## usage

### working with CIDs

```ocaml
open Ipld

(* Create a CID from data *)
let cid = Cid.create Cid.Dcbor data_bytes

(* Encode CID to base32 string *)
let cid_string = Cid.to_string cid
(* => "bafyreihffx5a2e7k5uwrmmgofbvzujc5cmw5h4espouwuxt3liqoflx3ee" *)

(* Decode CID from string *)
match Cid.of_string cid_string with
| Ok cid -> (* use cid *)
| Error msg -> failwith msg

(* Convert to/from bytes *)
let bytes = Cid.to_bytes cid
match Cid.of_bytes bytes with
| Ok cid -> (* use cid *)
| Error msg -> failwith msg
```

### DAG-CBOR encoding

```ocaml
open Dag_cbor

(* Create CBOR values *)
let value = `Map (String_map.of_list [
  ("name", `String "Alice");
  ("age", `Integer 30L);
  ("verified", `Boolean true);
  ("profile_cid", `Link some_cid);
])

(* Encode to bytes *)
let encoded = Dag_cbor.encode value

(* Decode from bytes *)
let decoded = Dag_cbor.decode encoded
```

### CAR files

CAR (Content Addressable aRchive) files store multiple IPLD blocks with their CIDs.

```ocaml
open Car

(* Write blocks to a CAR file *)
let blocks = [
  (cid1, data1);
  (cid2, data2);
  (cid3, data3);
] in
Car.write ~roots:[root_cid] blocks output_channel

(* Read blocks from a CAR file *)
let (roots, blocks) = Car.read input_channel
```

## types

### CID

```ocaml
type Cid.t = {
  version: int;        (* Always 1 for CIDv1 *)
  codec: codec;        (* Raw or Dcbor *)
  digest: digest;      (* SHA-256 hash *)
  bytes: bytes;        (* Serialized CID *)
}

type codec = Raw | Dcbor
```

### DAG-CBOR

```ocaml
type Dag_cbor.value =
  | `Null
  | `Boolean of bool
  | `Integer of int64
  | `Float of float
  | `String of string
  | `Bytes of bytes
  | `Array of value array
  | `Map of value String_map.t
  | `Link of Cid.t
  | `Tag of int * value
```
