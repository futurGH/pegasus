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
 (synopsis "Atproto repo functionality")
 (allow_empty)
 (depends ocaml dune (cborl (>= 0.1.0)) (cid (>= 0.1.0)) (hacl-star (>= 0.7.2)) lwt (mtime (>= 2.1.0)) (multihash (>= 0.1.0)) (yojson (>= 3.0.0)) (alcotest :with-test)))

(package
 (name ipld)
 (synopsis "A DASL-compliant implementation of some IPLD formats")
 (description "Currently includes DAG-CBOR and CIDv1")
 (allow_empty)
 (depends ocaml dune (digestif (>= 1.2.0)) (multibase (>= 0.1.0)) (alcotest :with-test)))
