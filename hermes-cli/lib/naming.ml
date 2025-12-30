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

(* generate qualified variant name including last nsid segment to avoid conflicts *)
(* "app.bsky.embed.images#view" -> "ImagesView" *)
(* "app.bsky.embed.images" (no #) -> "Images" (refers to main) *)
(* "#localDef" -> "LocalDef" (no qualifier for local refs) *)
let qualified_variant_name_of_ref ref_str =
  match String.split_on_char '#' ref_str with
  | [nsid; def] ->
      (* external ref with def: use last segment of nsid as qualifier *)
      let segments = String.split_on_char '.' nsid in
      let qualifier =
        match List.rev segments with
        | last :: _ ->
            String.capitalize_ascii last
        | [] ->
            ""
      in
      qualifier ^ String.capitalize_ascii def
  | [nsid] when not (String.contains nsid '#') -> (
      (* just nsid, no # - refers to main def, use last segment *)
      let segments = String.split_on_char '.' nsid in
      match List.rev segments with
      | last :: _ ->
          String.capitalize_ascii last
      | [] ->
          "Unknown" )
  | _ ->
      (* local ref like "#foo" *)
      if String.length ref_str > 0 && ref_str.[0] = '#' then
        String.capitalize_ascii
          (String.sub ref_str 1 (String.length ref_str - 1))
      else String.capitalize_ascii ref_str

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

(** find common prefix segments from a list of NSIDs
	e.g. ["app.bsky.actor.defs"; "app.bsky.feed.defs"; "app.bsky.graph.defs"]
    -> ["app"; "bsky"] *)
let common_prefix_of_nsids nsids =
  match nsids with
  | [] ->
      []
  | first :: rest ->
      let first_segments = String.split_on_char '.' first in
      List.fold_left
        (fun prefix nsid ->
          let segments = String.split_on_char '.' nsid in
          let rec common acc l1 l2 =
            match (l1, l2) with
            | h1 :: t1, h2 :: t2 when h1 = h2 ->
                common (h1 :: acc) t1 t2
            | _ ->
                List.rev acc
          in
          common [] prefix segments )
        first_segments rest

(** generate shared module file name from NSIDs
    e.g. ["app.bsky.actor.defs"; "app.bsky.feed.defs"] with index 1
    -> "app_bsky_shared_1.ml" *)
let shared_file_name nsids index =
  let prefix = common_prefix_of_nsids nsids in
  let prefix_str = String.concat "_" prefix in
  prefix_str ^ "_shared_" ^ string_of_int index ^ ".ml"

(** generate shared module name from NSIDs
    e.g. ["app.bsky.actor.defs"; "app.bsky.feed.defs"] with index 1
    -> "App_bsky_shared_1" *)
let shared_module_name nsids index =
  let prefix = common_prefix_of_nsids nsids in
  let prefix_str = String.concat "_" prefix in
  String.capitalize_ascii (prefix_str ^ "_shared_" ^ string_of_int index)

(** generate a short type name for use in shared modules
    uses the last segment of the nsid as context
    e.g. nsid="app.bsky.actor.defs", def_name="viewerState"
    -> "actor_viewer_state" *)
let shared_type_name nsid def_name =
  let segments = String.split_on_char '.' nsid in
  let context =
    match List.rev segments with
    (* use second-last segment if last is "defs" *)
    | "defs" :: second :: _ ->
        second
    | last :: _ ->
        last
    | [] ->
        "unknown"
  in
  type_name (context ^ "_" ^ def_name)
