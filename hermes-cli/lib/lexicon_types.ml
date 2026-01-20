type string_spec =
  { format: string option
  ; min_length: int option
  ; max_length: int option
  ; min_graphemes: int option
  ; max_graphemes: int option
  ; known_values: string list option
  ; enum: string list option
  ; const: string option
  ; default: string option
  ; description: string option }

type integer_spec =
  { minimum: int option
  ; maximum: int option
  ; enum: int list option
  ; const: int option
  ; default: int option
  ; description: string option }

type boolean_spec =
  {const: bool option; default: bool option; description: string option}

type bytes_spec =
  {min_length: int option; max_length: int option; description: string option}

type blob_spec =
  {accept: string list option; max_size: int option; description: string option}

type cid_link_spec = {description: string option}

type array_spec =
  { items: type_def
  ; min_length: int option
  ; max_length: int option
  ; description: string option }

and property = {type_def: type_def; description: string option}

and object_spec =
  { properties: (string * property) list
  ; required: string list option
  ; nullable: string list option
  ; description: string option }

and ref_spec =
  { ref_: string (* e.g., "#localDef" or "com.example.defs#someDef" *)
  ; description: string option }

and union_spec =
  {refs: string list; closed: bool option; description: string option}

and token_spec = {description: string option}

and unknown_spec = {description: string option}

and params_spec =
  { properties: (string * property) list
  ; required: string list option
  ; description: string option }

and body_def =
  {encoding: string; schema: type_def option; description: string option}

and error_def = {name: string; description: string option}

and query_spec =
  { parameters: params_spec option
  ; output: body_def option
  ; errors: error_def list option
  ; description: string option }

and procedure_spec =
  { parameters: params_spec option
  ; input: body_def option
  ; output: body_def option
  ; errors: error_def list option
  ; description: string option }

and subscription_spec =
  { parameters: params_spec option
  ; message: body_def option
  ; errors: error_def list option
  ; description: string option }

and record_spec =
  { key: string (* "tid", "nsid", etc. *)
  ; record: object_spec
  ; description: string option }

and lex_permission =
  { resource: string
  ; extra: (string * Yojson.Safe.t) list }

and permission_set_spec =
  { title: string option
  ; title_lang: (string * string) list option
  ; detail: string option
  ; detail_lang: (string * string) list option
  ; permissions: lex_permission list
  ; description: string option }

and type_def =
  | String of string_spec
  | Integer of integer_spec
  | Boolean of boolean_spec
  | Bytes of bytes_spec
  | Blob of blob_spec
  | CidLink of cid_link_spec
  | Array of array_spec
  | Object of object_spec
  | Ref of ref_spec
  | Union of union_spec
  | Token of token_spec
  | Unknown of unknown_spec
  | Query of query_spec
  | Procedure of procedure_spec
  | Subscription of subscription_spec
  | Record of record_spec
  | PermissionSet of permission_set_spec

type def_entry = {name: string; type_def: type_def}

type lexicon_doc =
  { lexicon: int (* always 1 *)
  ; id: string (* nsid *)
  ; revision: int option
  ; description: string option
  ; defs: def_entry list }

type parse_result = (lexicon_doc, string) result
