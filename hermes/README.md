# hermes

is a set of libraries that work together to provide a type-safe XRPC client for atproto.

Hermes provides three components:

- **hermes** - Core library for making XRPC calls
- **hermes-cli** - Code generator for atproto lexicons
- **hermes_ppx** (optional) - PPX extension for ergonomic API calls

### table of contents

- [Quick Start](#quick-start)
- [Complete Example](#complete-example)
- [hermes](#hermes-lib)
    - [Session Management](#session-management)
    - [Making XRPC Calls](#making-xrpc-calls)
    - [Error Handling](#error-handling)
- [hermes-cli](#hermes-cli)
    - [Usage](#usage)
    - [Options](#options)
    - [Generated Code Structure](#generated-code-structure)
    - [Type Mappings](#type-mappings)
    - [Bytes Encoding](#bytes-encoding)
    - [Union Types](#union-types)
- [hermes_ppx](#hermes-ppx)
    - [Setup](#setup)
    - [`[%xrpc ...]`](#ppx-xrpc)
    - [`[@@xrpc_query]`](#ppx-xrpc-query)

## quick start

You'll need your lexicon `.json` files in a directory somewhere. Start by running `hermes-cli` to generate modules from your lexicons.

```bash
hermes-cli generate ./lexicons/ --output=./hermes_lexicons
```

You can find the full set of options for `hermes-cli` [here](#options).

A `hermes_lexicons` directory will be created with generated modules for each lexicon found in `./lexicons`. You can now use these modules in your code.

```ocaml
open Hermes_lexicons (* generated using hermes-cli *)
open Lwt.Syntax

let () = Lwt_main.run begin
  (* Create an unauthenticated client *)
  let client = Hermes.make_client ~service:"https://public.api.bsky.app" () in

  (* Make a query using the generated module *)
  let* profile = App.Bsky.Actor.Profile.call ~actor:"bsky.app" client in
  print_endline profile.display_name;
  Lwt.return_unit
end
```

## complete example

You can add the `hermes_ppx` extension ([here's how!](#setup)) for more ergonomic API calls.

```ocaml
open Hermes_lexicons (* generated using hermes-cli *)
open Lwt.Syntax

let main () =
  (* Set up credential manager with persistence *)
  let manager = Hermes.make_credential_manager ~service:"https://pds.example" () in

  Hermes.on_session_update manager (fun session ->
    let json = Hermes.session_to_yojson session in
    Yojson.Safe.to_file "session.json" json;
    Lwt.return_unit
  );

  (* Log in or resume session *)
  let* client =
    if Sys.file_exists "session.json" then
      let json = Yojson.Safe.from_file "session.json" in
      match Hermes.session_of_yojson json with
      | Ok session -> Hermes.resume manager ~session ()
      | Error _ -> failwith "Invalid session file"
    else
      Hermes.login manager
        ~identifier:"you.bsky.social"
        ~password:"your-app-password"
        ()
  in

  (* Fetch your profile *)
  let session = Hermes.get_session client |> Option.get in
  let* profile =
    [%xrpc get "app.bsky.actor.getProfile"]
      ~actor:session.did
      client
  in
  Printf.printf "Logged in as %s\n" profile.handle;

  (* Create a post *)
  let* _ =
    [%xrpc post "com.atproto.repo.createRecord"]
      ~repo:session.did
      ~collection:"app.bsky.feed.post"
      ~record:(`Assoc [
        ("$type", `String "app.bsky.feed.post");
        ("text", `String "Hello from Hermes!");
        ("createdAt", `String (Ptime.to_rfc3339 (Ptime_clock.now ())));
      ])
      client
  in
  print_endline "Post created!";
  Lwt.return_unit

let () = Lwt_main.run (main ())
```

<h2 id="hermes-lib">hermes</h2>

### session management

```ocaml
(* Unauthenticated client for public endpoints *)
let client = Hermes.make_client ~service:"https://public.api.bsky.app" ()

(* Authenticated client with credential manager *)
let manager = Hermes.make_credential_manager ~service:"https://bsky.social" ()

let%lwt client = Hermes.login manager
  ~identifier:"user.bsky.social"
  ~password:"app-password-here"
  ()

(* Get current session for persistence *)
let session = Hermes.get_session client

(* Save session to JSON *)
let json = Hermes.session_to_yojson session

(* Resume from saved session *)
let%lwt client = Hermes.resume manager ~session ()

(* Auto-save session to disk *)
let () = Hermes.on_session_update manager (fun session ->
  save_to_disk (Hermes.session_to_yojson session);
  Lwt.return_unit
)

(* Listen for session expiration *)
let () = Hermes.on_session_expired manager (fun () ->
  print_endline "session expired, log in again!";
  Lwt.return_unit
)
```

### making XRPC calls

```ocaml
(* GET request *)
let%lwt result = Hermes.query client
  "app.bsky.actor.getProfile"
  (`Assoc [("actor", `String "bsky.app")])
  decode_profile

(* GET request returning raw bytes *)
let%lwt (data, content_type) = Hermes.query_bytes client
  "com.atproto.sync.getBlob"
  (`Assoc [("did", `String did); ("cid", `String cid)])

(* POST request *)
let%lwt result = Hermes.procedure client
  "com.atproto.repo.createRecord"
  (`Assoc [])  (* query params *)
  (Some input_json)
  decode_response

(* POST request with raw bytes as input *)
let%lwt response = Hermes.procedure_bytes client
  "com.atproto.repo.importRepo"
  (`Assoc [])
  (Some car_data)
  ~content_type:"application/vnd.ipld.car"

(* upload bytes, get a blob back *)
let%lwt blob = Hermes.procedure_blob client
  "com.atproto.repo.uploadBlob"
  (`Assoc [])
  image_bytes
  ~content_type:"image/jpeg"
  decode_blob
```

### error handling

```ocaml
try%lwt
  let%lwt _ = some_xrpc_call client in
  Lwt.return_unit
with Hermes.Xrpc_error { status; error; message } ->
  Printf.printf "Error %d: %s (%s)\n"
    status error (Option.value message ~default:"no message");
  Lwt.return_unit
```

<h2 id="hermes-cli">hermes-cli (codegen)</h2>

generates type-safe OCaml modules from atproto lexicon files.

### usage

```bash
# Generate from a lexicons directory
hermes-cli generate ./lexicons -o ./lib/generated

# Generate from multiple inputs
hermes-cli generate ./lexicons/com/atproto ./lexicons/app/bsky/feed/*.json -o ./lib/generated

# With custom root module name
hermes-cli generate ./lexicons -o ./lib/generated -m Bsky_api
```

### options

| Option          | Short | Description                              |
| --------------- | ----- | ---------------------------------------- |
| `INPUT...`      |       | Lexicon files or directories (recursive) |
| `--output`      | `-o`  | Output directory for generated OCaml     |
| `--module-name` | `-m`  | Root module name (default: Lexicons)     |

### generated code structure

For a lexicon like `app.bsky.actor.getProfile`, the generator creates:

```
lib/generated/
├── dune
├── lexicons.ml           # Re-exports all modules
└── app_bsky_actor_getProfile.ml
```

Each endpoint module contains:

```ocaml
module Main = struct
  type params = {
    actor: string;
    limit: int option [@default None];
  } [@@xrpc_query]

  type output = {
    did: string;
    handle: string;
    display_name: string option;
    (* ... *)
  } [@@deriving yojson]

  let nsid = "app.bsky.actor.getProfile"

  let call ~actor ?limit (client : Hermes.client) : output Lwt.t =
    let params = { actor; limit } in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
```

The generated `dune` file pulls in the required preprocessors:

```lisp
(library
 (name lexicons)
 (libraries hermes yojson lwt)
 (preprocess (pps hermes_ppx ppx_deriving_yojson)))
```

### type mappings

| Lexicon Type | OCaml Type      |
| ------------ | --------------- |
| `boolean`    | `bool`          |
| `integer`    | `int`           |
| `string`     | `string`        |
| `bytes`      | `string`        |
| `blob`       | `Hermes.blob`   |
| `cid-link`   | `Cid.t`         |
| `array`      | `list`          |
| `object`     | record type     |
| `union`      | variant type    |
| `unknown`    | `Yojson.Safe.t` |

### bytes encoding

Endpoints with non-JSON encoding are automatically detected and handled by `hermes-cli`:

- Queries with bytes output (e.g., `com.atproto.sync.getBlob`): output is `bytes * string` (data, content_type)
- Procedures with bytes input: input is `?input:bytes`

### union types

Unions generate variant types with a discriminator:

```ocaml
type relationship_union =
  | Relationship of relationship
  | NotFoundActor of not_found_actor
  | Unknown of Yojson.Safe.t  (* for open unions *)
```

<h2 id="hermes-ppx">hermes_ppx (PPX extension)</h2>

provides two rewrites: `[%xrpc ...]` for ergonomic API calls, and `[@@xrpc_query]` for XRPC server implementations.

### setup

```lisp
(library
 (name my_app)
 (libraries hermes hermes_ppx lwt)
 (preprocess (pps hermes_ppx ppx_deriving_yojson)))
```

If you use `[@@xrpc_query]`, you must include the `ppx_deriving_yojson` preprocessor after the `hermes_ppx` preprocessor.

<h3 id="ppx-xrpc"><code>[%xrpc ...]</code></h3>

transforms `[%xrpc ...]` into generated module calls.

```ocaml
let get_followers ~actor ~limit client =
  [%xrpc get "app.bsky.graph.getFollowers"]
    ~actor
    ?limit
    client

let create_post ~text client =
  let session = Hermes.get_session client |> Option.get in
  [%xrpc post "com.atproto.repo.createRecord"]
    ~repo:session.did
    ~collection:"app.bsky.feed.post"
    ~record:(`Assoc [
      ("$type", `String "app.bsky.feed.post");
      ("text", `String text);
      ("createdAt", `String (Ptime.to_rfc3339 (Ptime_clock.now ())));
    ])
    client
```

<h3 id="ppx-xrpc-query"><code>[@@xrpc_query]</code></h3>

allows parsing query strings to yojson.

```ocaml
type params = {
  actor: string;
  limit: int option [@default None];
  collections: string list option [@default None];
} [@@xrpc_query]
```

Under the hood, the PPX assigns custom to_yojson/of_yojson functions to the record's fields, allowing for correctly parsing from an `Assoc` containing string or string list entries to the types specified in the record.
