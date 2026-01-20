(lang dune 3.20)
(using melange 0.1)

(name pegasus)

(generate_opam_files true)

(source (github futurGH/pegasus))
(authors "futurGH")
(maintainers "futurGH")

(license MPL-2.0)

(pin ; fixes build on macOS 26
 (url "git+https://github.com/mcksan/pg_query-ocaml.git")
 (package (name pg_query)))

(pin ; build broken on macOS for some reason
 (url "git+https://github.com/janestreet/bin_prot.git#03c783d682f67db665dca44a1b34d41cd16e1e3f")
 (package
   (name bin_prot)
   (version v0.17~preview.128.20+135))) ; this is actually v0.16~preview.128.20+135 but core requires >=v0.17

(pin ; these 4 all need to be pinned or they'll conflict
 (url "git+https://github.com/lthms/dream.git#h2-0-13") ; unmerged patch that prevents conflict with letters
 (package (name dream)))
 (pin
  (url "git+https://github.com/lthms/dream.git#h2-0-13") ; unmerged patch that prevents conflict with letters
  (package (name dream-httpaf)))
(pin
 (url "git+https://github.com/roddyyaga/ppx_rapper.git#5b0e62def2d5cc6cbe3dedec1ecb289bee350f9a")
 (package (name ppx_rapper)))
(pin
 (url "git+https://github.com/roddyyaga/ppx_rapper.git#5b0e62def2d5cc6cbe3dedec1ecb289bee350f9a")
 (package (name ppx_rapper_lwt)))

(pin ; applied fix from anmonteiro/gluten#85
 (url "git+https://github.com/futurGH/gluten#956e753dc0ee81dbe53f0979fe89d57d960e7856")
 (package
  (name gluten-lwt)
  (version 0.5.2)))

(pin ; version on opam requires too high a melange version
 (url "git+https://github.com/melange-community/melange-webapi.git#80c6ededd06cc66b75445d1ed5c855e050b156a0")
 (package
   (name melange-webapi)
   (version v0.21.0)))

(package
 (name pegasus)
 (synopsis "An atproto Personal Data Server implementation")
 (description "Eventually")
 (allow_empty)
 (depends
  (ocaml (= 5.2.1))
  (dune (and (>= 3.20) (< 3.21)))
  lwt
  (aws-s3-lwt (>= 4.8.1))
  (caqti (>= 1.9.0))
  (caqti-driver-sqlite3 (>= 1.9.0))
  (caqti-lwt (>= 1.9.0))
  (cohttp (>= 6.1.1))
  (cohttp-lwt-unix (>= 6.1.1))
  (dns-client-lwt (>= 10.2.0))
  dream
  (emile (>= 1.1))
  (letters (>= 0.4.0))
  (html_of_jsx (>= 0.0.7))
  (re (>= 1.13.2))
  (safepass (>= 3.1))
  server-reason-react
  (timedesc (>= 3.1.0))
  (uri (>= 4.4.0))
  (uuidm (>= 0.9.10))
  (yojson (>= 3.0.0))
  (lwt_ppx (>= 5.9.1))
  (ppx_deriving_yojson (>= 3.9.1))
  ppx_rapper
  ppx_rapper_lwt
  (webauthn (>= 0.2.0))
  (alcotest :with-test)
  crunch
  (ocamlformat-mlx :with-dev-setup)
  (ocamlmerlin-mlx :with-dev-setup)))

(package
 (name frontend)
 (allow_empty)
 (depends
  (ocaml (= 5.2.1))
  (dune (and (>= 3.20) (< 3.21)))
  lwt
  melange
  melange-json
  melange-json-native
  (mlx (>= 0.11))
  (reason-react (>= 0.16.0))
  (reason-react-ppx (>= 0.16.0))
  server-reason-react))

(package
 (name mist)
 (synopsis "Atproto repo functionality")
 (allow_empty)
 (depends
  (ocaml (= 5.2.1))
  (dune (and (>= 3.20) (< 3.21)))
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
  (ocaml (= 5.2.1))
  (dune (and (>= 3.20) (< 3.21)))
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
  (ocaml (= 5.2.1))
  (dune (and (>= 3.20) (< 3.21)))
  (hacl-star (>= 0.7.2))
  (mirage-crypto-ec (>= 2.0.1))
  (multibase (>= 0.1.0))))

(package
 (name hermes)
 (synopsis "Type-safe XRPC client for ATProto")
 (description "XRPC client with PPX extensions for type-safe API calls")
 (allow_empty)
 (depends
  (ocaml (= 5.2.1))
  (dune (and (>= 3.20) (< 3.21)))
  lwt
  (cohttp-lwt-unix (>= 6.1.1))
  (uri (>= 4.4.0))
  (yojson (>= 3.0.0))
  (base64 (>= 3.5.0))
  (lwt_ppx (>= 5.9.1))
  (ppx_deriving_yojson (>= 3.9.1))))

(package
 (name hermes-cli)
 (synopsis "Code generator for Hermes from ATProto lexicons")
 (allow_empty)
 (depends
  (ocaml (= 5.2.1))
  (dune (and (>= 3.20) (< 3.21)))
  (cmdliner (>= 1.2.0))
  (yojson (>= 3.0.0))
  (fmt (>= 0.9.0))
  (fpath (>= 0.7.3))))

(package
 (name hermes_ppx)
 (synopsis "PPX extension for Hermes XRPC calls")
 (allow_empty)
 (depends
  (ocaml (= 5.2.1))
  (dune (and (>= 3.20) (< 3.21)))
  (ppxlib (>= 0.32.0))))

(package
 (name tailwindcss)
 (allow_empty))

(dialect
 (name mlx)
 (implementation
  (extension mlx)
  (merlin_reader mlx)
  (preprocess
   (run mlx-pp %{input-file}))))
