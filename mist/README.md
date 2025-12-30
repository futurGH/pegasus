# mist

is a [Merkle Search Tree](https://atproto.com/specs/repository#mst-structure) implementation for atproto data repositories.

## installation

Add to your `dune-project`:

```lisp
(depends
  mist
  ipld)  ; required dependency
```

## usage

### working with TIDs

TIDs are 13-character base32-encoded identifiers that combine a microsecond timestamp with a clock ID for ordering and uniqueness.

```ocaml
open Mist

(* Generate a TID from current timestamp *)
let tid = Tid.now ()
(* => "3jzfcijpj2z23" *)

(* Create TID from timestamp *)
let tid = Tid.of_timestamp_ms 1609459200000L
  ~clockid:123

(* Parse TID from string *)
let tid = Tid.of_string "3jzfcijpj2z23"

(* Extract timestamp *)
let (timestamp_us, clockid) = Tid.to_timestamp_us tid

(* TIDs are comparable for ordering *)
let is_later = Tid.compare tid1 tid2 > 0
```

### working with MSTs

```ocaml
open Lwt.Syntax

(* Create a new MST with a blockstore and an empty root *)
let blockstore = Mist.Storage.Memory_blockstore.create () in
let* mst = Mst.create blockstore (Cid.of_string "")

(* Add an entry *)
let key = "app.bsky.feed.post/3jzfcijpj2z23" in
let cid = Cid.of_string "bafy2bzaceb3z2z23" in
let* mst = Mst.add mst key cid blockstore

(* Get an entry *)
let* value_opt = Mst.retrieve_node mst cid in
match value_opt with
| Some node -> (* found *)
| None -> (* not found *)

(* Delete an entry *)
let* mst = Mst.delete mst key

(* Get the root CID *)
let root_cid = Cid.to_string mst.root
```

### inductive proof

```ocaml
(* Generate a map of all blocks needed to prove a given key *)
let* proof = Mst.proof_for_key mst cid key in
```

### working with blob references

```ocaml
(* Parse blob reference from JSON *)
let blob = Blob_ref.of_yojson json

(* Access blob properties *)
let cid = blob.ref in
let mime_type = blob.mime_type in
let size = blob.size

(* Convert to IPLD representation *)
let ipld = Blob_ref.to_ipld blob

(* Convert back to JSON *)
let json = Blob_ref.to_yojson blob
```
