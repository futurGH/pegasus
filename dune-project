(lang dune 3.14)

(name pegasus)

(generate_opam_files true)

(source (github futurGH/pegasus))
(authors "futurGH")
(maintainers "futurGH")

(license MPL-2.0)

(package
 (name pegasus)
 (synopsis "An atproto Personal Data Server implementation")
 (description "Eventually")
 (allow_empty)
 (depends ocaml dune))

(package
 (name mist)
 (synopsis "A Merkle Search Tree implementation")
 (allow_empty)
 (depends ocaml dune (cborl (>= 0.1.0)) (cid (>= 0.1.0)) lwt (multihash (>= 0.1.0)) (alcotest :with-test)))

(package
 (name dag_cbor)
 (synopsis "An IPLD DAG-CBOR implementation")
 (allow_empty)
 (depends ocaml dune (cid (>= 0.1.0)) (alcotest :with-test)))
