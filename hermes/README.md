# hermes

is a type-safe XRPC client for atproto.

Hermes provides three components:

- **hermes** - Core library for making XRPC calls
- **hermes-cli** - Code generator for atproto lexicons
- **hermes_ppx** - PPX extension for ergonomic API calls

- [Quick Start](#quick-start)
- [Complete Example](#complete-example)
- [Installation](#installation)
- [hermes](#hermes-lib)
    - [Session Management](#session-management)
    - [Making XRPC Calls](#making-xrpc-calls)
    - [Error Handling](#error-handling)
- [hermes_ppx](#hermes-ppx)
    - [Setup](#setup)
    - [Usage](#ppx-usage)
- [hermes-cli](#hermes-cli)
    - [Usage](#usage)
    - [Options](#options)
    - [Generated Code Structure](#generated-code-structure)
    - [Type Mappings](#type-mappings)
    - [Bytes Encoding](#bytes-encoding)
    - [Union Types](#union-types)

## quick start

```ocaml
open Hermes_lexicons (* generate lexicons using hermes-cli! *)
open Lwt.Syntax

let () = Lwt_main.run begin
  (* Create an unauthenticated client *)
  let client = Hermes.make_client ~service:"https://public.api.bsky.app" () in

  (* Make a query using the generated module *)
  let* profile = App_bsky_actor_getProfile.call ~actor:"bsky.app" client in
  print_endline profile.display_name;
  Lwt.return_unit
end
```

## complete example

```ocaml
open Hermes_lexicons (* generate lexicons using hermes-cli! *)
open Lwt.Syntax

let main () =
  (* Set up credential manager with persistence *)
  let manager = Hermes.make_credential_manager ~service:"https://pegasus.example" () in

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

## installation

Add to your `dune-project`:

```lisp
(depends
  hermes
  hermes_ppx)
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
# Generate from lexicons directory
hermes-cli generate --input ./lexicons --output ./lib/generated

# With custom root module name
hermes-cli generate -i ./lexicons -o ./lib/generated --module-name Bsky_api
```

### options

| Option          | Short | Description                             |
| --------------- | ----- | --------------------------------------- |
| `--input`       | `-i`  | Directory containing lexicon JSON files |
| `--output`      | `-o`  | Output directory for generated OCaml    |
| `--module-name` | `-m`  | Root module name (default: Lexicons)    |

### generated code structure

For a lexicon like `app.bsky.actor.getProfile`, the generator creates:

```
lib/generated/
├── dune
├── lexicons.ml           # Re-exports all modules
└── app/
    └── bsky/
        └── actor/
            └── getProfile.ml
```

Each endpoint module contains:

```ocaml
module GetProfile = struct
  type params = {
    actor: string;
  } [@@deriving yojson]

  type output = {
    did: string;
    handle: string;
    display_name: string option;
    (* ... *)
  } [@@deriving yojson]

  let nsid = "app.bsky.actor.getProfile"

  let call ~actor (client : Hermes.client) : output Lwt.t =
    let params = { actor } in
    Hermes.query client nsid (params_to_yojson params) output_of_yojson
end
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

Endpoints with non-JSON encoding are automatically detected and handled:

- **Queries with bytes output** (e.g., `com.atproto.sync.getBlob` with `encoding: "*/*"`):
    - Output type is `string * string` (data, content_type)
    - Generated code uses `Hermes.query_bytes`

- **Procedures with bytes input**:
    - Input is `?input:string` (optional raw bytes)
    - Generated code uses `Hermes.procedure_bytes`

### union types

Unions generate variant types with a discriminator:

```ocaml
type relationship_union =
  | Relationship of relationship
  | NotFoundActor of not_found_actor
  | Unknown of Yojson.Safe.t  (* for open unions *)
```

<h2 id="hermes-ppx">hermes_ppx (PPX extension)</h2>

Transforms `[%xrpc ...]` into generated module calls.

### setup

```lisp
(library
 (name my_app)
 (libraries hermes hermes_ppx lwt)
 (preprocess (pps hermes_ppx)))
```

<h3 id="ppx-usage">usage</h3>

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
