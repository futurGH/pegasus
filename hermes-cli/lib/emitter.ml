type t =
  { mutable imports: string list
  ; mutable generated_unions: string list
  ; mutable union_names: (string list * string) list (* refs -> context name *)
  ; buf: Buffer.t }

let make () =
  {imports= []; generated_unions= []; union_names= []; buf= Buffer.create 4096}

(** add an import if not already present *)
let add_import t module_name =
  if not (List.mem module_name t.imports) then
    t.imports <- module_name :: t.imports

let get_imports t = t.imports

(** mark a union type as generated to avoid duplicates *)
let mark_union_generated t union_name =
  if not (List.mem union_name t.generated_unions) then
    t.generated_unions <- union_name :: t.generated_unions

let is_union_generated t union_name = List.mem union_name t.generated_unions

(** register a context-based name for a union based on its refs,
    allowing inline unions to be reused when the same refs appear elsewhere *)
let register_union_name t refs context_name =
  let sorted_refs = List.sort String.compare refs in
  if not (List.exists (fun (r, _) -> r = sorted_refs) t.union_names) then
    t.union_names <- (sorted_refs, context_name) :: t.union_names

(** look up a union's registered context-based name *)
let lookup_union_name t refs =
  let sorted_refs = List.sort String.compare refs in
  List.assoc_opt sorted_refs t.union_names

let emit t s = Buffer.add_string t.buf s

let emitln t s = Buffer.add_string t.buf s ; Buffer.add_char t.buf '\n'

let emit_newline t = Buffer.add_char t.buf '\n'

let contents t = Buffer.contents t.buf
