(* ocaml reserved keywords that need escaping *)
let reserved_keywords =
  [ "and"
  ; "as"
  ; "assert"
  ; "asr"
  ; "begin"
  ; "class"
  ; "constraint"
  ; "do"
  ; "done"
  ; "downto"
  ; "else"
  ; "end"
  ; "exception"
  ; "external"
  ; "false"
  ; "for"
  ; "fun"
  ; "function"
  ; "functor"
  ; "if"
  ; "in"
  ; "include"
  ; "inherit"
  ; "initializer"
  ; "land"
  ; "lazy"
  ; "let"
  ; "lor"
  ; "lsl"
  ; "lsr"
  ; "lxor"
  ; "match"
  ; "method"
  ; "mod"
  ; "module"
  ; "mutable"
  ; "new"
  ; "nonrec"
  ; "object"
  ; "of"
  ; "open"
  ; "or"
  ; "private"
  ; "rec"
  ; "sig"
  ; "struct"
  ; "then"
  ; "to"
  ; "true"
  ; "try"
  ; "type"
  ; "val"
  ; "virtual"
  ; "when"
  ; "while"
  ; "with" ]

let is_reserved name = List.mem (String.lowercase_ascii name) reserved_keywords

(* convert camelCase to snake_case *)
let camel_to_snake s =
  let buf = Buffer.create (String.length s * 2) in
  String.iteri
    (fun i c ->
      if Char.uppercase_ascii c = c && c <> Char.lowercase_ascii c then begin
        if i > 0 then Buffer.add_char buf '_' ;
        Buffer.add_char buf (Char.lowercase_ascii c)
      end
      else Buffer.add_char buf c )
    s ;
  Buffer.contents buf

let escape_keyword name = if is_reserved name then name ^ "_" else name

let field_name name = escape_keyword (camel_to_snake name)

let module_name_of_segment segment =
  if String.length segment = 0 then segment else String.capitalize_ascii segment

let module_path_of_nsid nsid =
  String.split_on_char '.' nsid |> List.map module_name_of_segment

let type_name_of_nsid nsid =
  let segments = String.split_on_char '.' nsid in
  match List.rev segments with
  | last :: _ ->
      camel_to_snake last
  | [] ->
      "unknown"

let type_name name = escape_keyword (camel_to_snake name)

let def_module_name name = String.capitalize_ascii name

(* generate variant constructor name from ref *)
let variant_name_of_ref ref_str =
  (* "#localDef" -> "LocalDef", "com.example.defs#someDef" -> "SomeDef" *)
  let name =
    match String.split_on_char '#' ref_str with
    | [_; def] ->
        def
    | [def] -> (
      (* just nsid, use last segment *)
      match List.rev (String.split_on_char '.' def) with
      | last :: _ ->
          last
      | [] ->
          "Unknown" )
    | _ ->
        "Unknown"
  in
  String.capitalize_ascii name

let union_type_name refs =
  match refs with
  | [] ->
      "unknown_union"
  | [r] ->
      type_name (variant_name_of_ref r)
  | _ -> (
      (* use first two refs to generate a name *)
      let names = List.map variant_name_of_ref refs in
      let sorted = List.sort String.compare names in
      match sorted with
      | a :: b :: _ ->
          camel_to_snake a ^ "_or_" ^ camel_to_snake b
      | [a] ->
          camel_to_snake a
      | [] ->
          "unknown_union" )

(* convert nsid to flat file path and module name *)
let flat_name_of_nsid nsid = String.split_on_char '.' nsid |> String.concat "_"

let file_path_of_nsid nsid = flat_name_of_nsid nsid ^ ".ml"

let flat_module_name_of_nsid nsid =
  String.capitalize_ascii (flat_name_of_nsid nsid)

let needs_key_annotation original_name ocaml_name = original_name <> ocaml_name

let key_annotation original_name ocaml_name =
  if needs_key_annotation original_name ocaml_name then
    Printf.sprintf " [@key \"%s\"]" original_name
  else ""
