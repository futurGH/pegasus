# pegasus

is an [atproto PDS](https://atproto.com/guides/glossary#pds-personal-data-server), along with an assortment of atproto-relevant libraries, written in OCaml.

## table of contents

- [Running It](#running-it)
- [Environment](#environment)
    - [SMTP](#smtp)
    - [S3](#s3)
- [Development](#development)
- [Libraries](#libraries)
    - [ipld](#ipld) - IPLD implementation (CIDs, CAR, DAG-CBOR)
    - [kleidos](#kleidos) - Cryptographic key management
    - [mist](#mist) - Merkle Search Tree implementation
    - [hermes](#hermes) - XRPC client
    - [frontend](#frontend) - Web interface
    - [pegasus](#pegasus-library) - PDS implementation

## running it

After cloning this repo, start by running

```
docker compose pull
```

to pull the latest image, or

```
docker compose build
```

to build from source.

Next, run

```
docker compose run --rm --entrypoint gen-keys pds
```

to generate some of the environment variables you'll need.

Copy [`.env.example`](.env.example) to `.env` and fill in the environment variables marked as required. See [Environment](#environment) for further details on configuration.

After that, run

```
docker compose up -d
```

to start the PDS, then navigate to `https://{PDS_HOSTNAME}/admin` to log in with the admin password you specified and create an invite code or a new account on the PDS.

## environment

Documentation for most environment variables can be found in [`.env.example`](.env.example). There are two optional categories of environment variables that add functionality:

### SMTP

The PDS can email users for password changes, identity updates, and account deletion. If these environment variables are not set, emails will instead be logged to the process' stdout.

- `PDS_SMTP_AUTH_URI` — The URI to connect to the mail server. This should look like `smtp[s]://user:pass@host[:port]`.
- `PDS_SMTP_STARTTLS=false` — Whether to use STARTTLS when connecting to the mail server. Defaults to false. If true, the connection will default to port 587. If false, the connection will default to port 465. Setting a port in `PDS_SMTP_AUTH_URI` will override either one.
- `PDS_SMTP_SENDER` — The identity to send emails as. Can be an email address (`e@mail.com`) or a mailbox (`Name <e@mail.com>`).

### S3

The PDS can be configured to back up server data to and/or store blobs in S3(-compatible storage).

- `PDS_S3_BLOBS_ENABLED=false` — Whether to store blobs in S3. By default, blobs are stored locally in `{PDS_DATA_DIR}/blobs/[did]/`.
- `PDS_S3_BACKUPS_ENABLED=false` — Whether to back up data to S3.
- `PDS_S3_BACKUP_INTERVAL_S=3600` — How often to back up to S3, in seconds.
- `PDS_S3_ENDPOINT`, `PDS_S3_REGION`, `PDS_S3_BUCKET`, `PDS_S3_ACCESS_KEY`, `PDS_S3_SECRET_KEY` — S3 configuration.
- `PDS_S3_CDN_URL` — You may optionally set this to redirect `getBlob` requests to `{PDS_S3_CDN_URL}/blobs/{did}/{cid}`. When unset, blobs will be fetched either from local storage or from S3, depending on `PDS_S3_BLOBS_ENABLED`.

## libraries

This repo contains several libraries in addition to the `pegasus` PDS. Each library has its own README with detailed documentation.

### <a id="ipld"></a>[ipld](ipld/README.md)

A mostly [DASL-compliant](https://dasl.ing/) implementation of [CIDs](https://dasl.ing/cid.html), [CAR](https://dasl.ing/car.html), and [DAG-CBOR](https://dasl.ing/drisl.html).

Provides content addressing primitives for IPLD: Content Identifiers (CIDs), Content Addressable aRchives (CAR), and deterministic CBOR encoding.

### <a id="kleidos"></a>[kleidos](kleidos/README.md)

An atproto-valid interface for secp256k1 and secp256r1 key management, signing/verifying, and encoding/decoding.

Handles cryptographic operations for both K-256 and P-256 elliptic curves with multikey encoding and did:ket generation.

### <a id="mist"></a>[mist](mist/README.md)

A [Merkle Search Tree](https://atproto.com/specs/repository#mst-structure) implementation for data repository purposes with a swappable storage backend.

### <a id="hermes"></a>[hermes](hermes/README.md)

An XRPC client for atproto with three components:

- **hermes** - Core XRPC client library
- **hermes_ppx** - PPX extension for ergonomic API calls
- **hermes-cli** - CLI to generate OCaml types from atproto lexicons

### <a id="frontend"></a>[frontend](frontend/README.md)

The PDS frontend, containing the admin dashboard and account page.

### <a id="pegasus-library"></a>[pegasus](pegasus/README.md)

The PDS implementation.

## development

To start developing, you'll need:

- [`opam`](https://opam.ocaml.org/doc/Install.html), the OCaml Package Manager
- and the following packages, or their equivalents on your operating system: `cmake git libev-dev libffi-dev libgmp-dev libssl-dev libsqlite3-dev libpcre3-dev pkg-config`

Start by creating an opam switch; similar to a Python virtual environment, storing the dependencies for this project and a specific compiler version. After that, install [`dune`](https://dune.build), the build system/package manager pegasus uses.

```
opam switch create . 5.2.1 --no-install
opam install dune
```

You may need to run `eval $(opam env)` for this to work. Next, run

```
dune pkg lock
```

to solve dependencies.

Set the required environment variables (see [Environment](#environment)), noting that the program won't automatically read from `.env`, then either run

```
dune exec pegasus
```

to run the program directly, or

```
dune build
```

to produce an executable that you'll likely find in `_build/default/bin`.

For development, you'll also want to run

```
dune tools exec ocamlformat
dune tools exec ocamllsp
```

to download the formatter and LSP services. You can run `dune fmt` to format the project.

The [frontend](frontend/) is written in [MLX](https://github.com/ocaml-mlx/mlx), a JSX-ish OCaml dialect. To format it, you'll need to `opam install ocamlformat-mlx`, then `ocamlformat-mlx -i frontend/**/*.mlx`. You'll see a few errors on formatting files containing `[%browser_only]`; I'm waiting on the next release of `mlx` to fix those.
