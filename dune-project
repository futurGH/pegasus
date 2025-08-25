(lang dune 3.14)

(name pegasus)

(generate_opam_files true)

(source (github futurGH/pegasus))
(authors "futurGH")
(maintainers "futurGH")

(license MPL-2.0)

(pin
 (url "git+https://github.com/aantron/dream.git")
 (package (name dream)))

(pin
 (url "git+https://github.com/roddyyaga/ppx_rapper.git")
 (package (name ppx_rapper)))

(pin
 (url "git+https://github.com/roddyyaga/ppx_rapper.git")
 (package (name ppx_rapper_lwt)))

(package
 (name pegasus)
 (synopsis "An atproto Personal Data Server implementation")
 (description "Eventually")
 (allow_empty)
 (depends
  ocaml
  dune
  lwt
  (caqti (>= 1.9.0))
  (caqti-driver-sqlite3 (>= 1.9.0))
  (caqti-lwt (>= 1.9.0))
  dream
  (re (>= 1.13.2))
  (safepass (>= 3.1))
  (yojson (>= 3.0.0))
  (lwt_ppx (>= 5.9.1))
  (ppx_deriving_yojson (>= 3.9.1))
  ppx_rapper
  ppx_rapper_lwt
  (alcotest :with-test)))

(package
 (name mist)
 (synopsis "Atproto repo functionality")
 (allow_empty)
 (depends
  ocaml
  dune
  lwt
  (core_unix (>= 0.16.0))
  (re (>= 1.13.2))
  (yojson (>= 3.0.0))
  (lwt_ppx (>= 5.9.1))
  (ppx_deriving_yojson (>= 3.9.1))
  (alcotest :with-test)))

(package
 (name ipld)
 (synopsis "A DASL-compliant implementation of some IPLD formats")
 (description "Currently includes DAG-CBOR, CIDv1, and CARv1")
 (allow_empty)
 (depends
  ocaml
  dune
  lwt
  (digestif (>= 1.2.0))
  (multibase (>= 0.1.0))
  (yojson (>= 3.0.0))
  (lwt_ppx (>= 5.9.1))
  (alcotest :with-test)))

(package
 (name kleidos)
 (synopsis "Atproto-flavour k256 and p256 signing and verification")
 (allow_empty)
 (depends
  ocaml
  dune
  (hacl-star (>= 0.7.2))
  (mirage-crypto-ec (>= 2.0.1))
  (multibase (>= 0.1.0))))
