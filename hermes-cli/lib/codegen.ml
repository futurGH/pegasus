open Lexicon_types

(* use Emitter module for output buffer management *)
type output = Emitter.t

let make_output = Emitter.make

let add_import = Emitter.add_import

let mark_union_generated = Emitter.mark_union_generated

let is_union_generated = Emitter.is_union_generated

let register_union_name = Emitter.register_union_name

let lookup_union_name = Emitter.lookup_union_name

let emit = Emitter.emit

let emitln = Emitter.emitln

let emit_newline = Emitter.emit_newline

(* generate ocaml type for a primitive type *)
let rec gen_type_ref nsid out (type_def : type_def) : string =
  match type_def with
  | String _ ->
      "string"
  | Integer {maximum; _} -> (
    (* use int64 for large integers *)
    match maximum with
    | Some m when m > 1073741823 ->
        "int64"
    | _ ->
        "int" )
  | Boolean _ ->
      "bool"
  | Bytes _ ->
      "bytes"
  | Blob _ ->
      "Hermes.blob"
  | CidLink _ ->
      "Cid.t"
  | Array {items; _} ->
      let item_type = gen_type_ref nsid out items in
      item_type ^ " list"
  | Object _ ->
      (* objects should be defined separately *)
      "object_todo"
  | Ref {ref_; _} ->
      gen_ref_type nsid out ref_
  | Union {refs; _} -> (
    (* generate inline union reference, using registered name if available *)
    match lookup_union_name out refs with
    | Some name ->
        name
    | None ->
        gen_union_type_name refs )
  | Token _ ->
      "string"
  | Unknown _ ->
      "Yojson.Safe.t"
  | Query _ | Procedure _ | Subscription _ | Record _ ->
      "unit (* primary type *)"

(* generate reference to another type *)
and gen_ref_type nsid out ref_str : string =
  if String.length ref_str > 0 && ref_str.[0] = '#' then begin
    (* local ref: #someDef -> someDef *)
    let def_name = String.sub ref_str 1 (String.length ref_str - 1) in
    Naming.type_name def_name
  end
  else begin
    (* external ref: com.example.defs#someDef *)
    match String.split_on_char '#' ref_str with
    | [ext_nsid; def_name] ->
        if ext_nsid = nsid then
          (* ref to same nsid, treat as local *)
          Naming.type_name def_name
        else begin
          (* use flat module names for include_subdirs unqualified *)
          let flat_module = Naming.flat_module_name_of_nsid ext_nsid in
          add_import out flat_module ;
          flat_module ^ "." ^ Naming.type_name def_name
        end
    | [ext_nsid] ->
        if ext_nsid = nsid then Naming.type_name "main"
        else begin
          (* just nsid, refers to main def *)
          let flat_module = Naming.flat_module_name_of_nsid ext_nsid in
          add_import out flat_module ; flat_module ^ ".main"
        end
    | _ ->
        "invalid_ref"
  end

and gen_union_type_name refs = Naming.union_type_name refs

(* generate full type uri for a ref *)
let gen_type_uri nsid ref_str =
  if String.length ref_str > 0 && ref_str.[0] = '#' then
    (* local ref *)
    nsid ^ ref_str
  else
    (* external ref, use as-is *)
    ref_str

(* collect inline union specs from object properties with context *)
let rec collect_inline_unions_with_context context acc type_def =
  match type_def with
  | Union spec ->
      (context, spec.refs, spec) :: acc
  | Array {items; _} ->
      (* for array items, append _item to context *)
      collect_inline_unions_with_context (context ^ "_item") acc items
  | _ ->
      acc

let collect_inline_unions_from_properties properties =
  List.fold_left
    (fun acc (prop_name, (prop : property)) ->
      collect_inline_unions_with_context prop_name acc prop.type_def )
    [] properties

(* generate inline union types that appear in object properties *)
let gen_inline_unions nsid out properties =
  let inline_unions = collect_inline_unions_from_properties properties in
  List.iter
    (fun (context, refs, spec) ->
      (* register and use context-based name *)
      let context_name = Naming.type_name context in
      register_union_name out refs context_name ;
      let type_name = context_name in
      (* skip if already generated *)
      if not (is_union_generated out type_name) then begin
        mark_union_generated out type_name ;
        let is_closed = Option.value spec.closed ~default:false in
        emitln out (Printf.sprintf "type %s =" type_name) ;
        List.iter
          (fun ref_str ->
            let variant_name = Naming.variant_name_of_ref ref_str in
            let payload_type = gen_ref_type nsid out ref_str in
            emitln out (Printf.sprintf "  | %s of %s" variant_name payload_type) )
          refs ;
        if not is_closed then emitln out "  | Unknown of Yojson.Safe.t" ;
        emit_newline out ;
        (* generate of_yojson function *)
        emitln out (Printf.sprintf "let %s_of_yojson json =" type_name) ;
        emitln out "  let open Yojson.Safe.Util in" ;
        emitln out "  try" ;
        emitln out "    match json |> member \"$type\" |> to_string with" ;
        List.iter
          (fun ref_str ->
            let variant_name = Naming.variant_name_of_ref ref_str in
            let full_type_uri = gen_type_uri nsid ref_str in
            let payload_type = gen_ref_type nsid out ref_str in
            emitln out (Printf.sprintf "    | \"%s\" ->" full_type_uri) ;
            emitln out
              (Printf.sprintf "        (match %s_of_yojson json with"
                 payload_type ) ;
            emitln out
              (Printf.sprintf "         | Ok v -> Ok (%s v)" variant_name) ;
            emitln out "         | Error e -> Error e)" )
          refs ;
        if is_closed then
          emitln out "    | t -> Error (\"unknown union type: \" ^ t)"
        else emitln out "    | _ -> Ok (Unknown json)" ;
        emitln out "  with _ -> Error \"failed to parse union\"" ;
        emit_newline out ;
        (* generate to_yojson function *)
        emitln out (Printf.sprintf "let %s_to_yojson = function" type_name) ;
        List.iter
          (fun ref_str ->
            let variant_name = Naming.variant_name_of_ref ref_str in
            let full_type_uri = gen_type_uri nsid ref_str in
            let payload_type = gen_ref_type nsid out ref_str in
            emitln out (Printf.sprintf "  | %s v ->" variant_name) ;
            emitln out
              (Printf.sprintf "      (match %s_to_yojson v with" payload_type) ;
            emitln out
              (Printf.sprintf
                 "       | `Assoc fields -> `Assoc ((\"$type\", `String \
                  \"%s\") :: fields)"
                 full_type_uri ) ;
            emitln out "       | other -> other)" )
          refs ;
        if not is_closed then emitln out "  | Unknown j -> j" ;
        emit_newline out
      end )
    inline_unions

(* generate object type definition *)
(* ~first: use "type" if true, "and" if false *)
(* ~last: add [@@deriving yojson] if true *)
let gen_object_type ?(first = true) ?(last = true) nsid out name
    (spec : object_spec) =
  let required = Option.value spec.required ~default:[] in
  let nullable = Option.value spec.nullable ~default:[] in
  let keyword = if first then "type" else "and" in
  (* handle empty objects as unit *)
  if spec.properties = [] then begin
    emitln out (Printf.sprintf "%s %s = unit" keyword (Naming.type_name name)) ;
    if last then begin
      emitln out
        (Printf.sprintf "let %s_of_yojson _ = Ok ()" (Naming.type_name name)) ;
      emitln out
        (Printf.sprintf "let %s_to_yojson () = `Assoc []"
           (Naming.type_name name) ) ;
      emit_newline out
    end
  end
  else begin
    (* generate inline union types first, but only if this is the first type *)
    if first then gen_inline_unions nsid out spec.properties ;
    emitln out (Printf.sprintf "%s %s =" keyword (Naming.type_name name)) ;
    emitln out "  {" ;
    List.iter
      (fun (prop_name, (prop : property)) ->
        let ocaml_name = Naming.field_name prop_name in
        let base_type = gen_type_ref nsid out prop.type_def in
        let is_required = List.mem prop_name required in
        let is_nullable = List.mem prop_name nullable in
        let type_str =
          if is_required && not is_nullable then base_type
          else base_type ^ " option"
        in
        let key_attr = Naming.key_annotation prop_name ocaml_name in
        let default_attr =
          if is_required && not is_nullable then "" else " [@default None]"
        in
        emitln out
          (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str key_attr
             default_attr ) )
      spec.properties ;
    emitln out "  }" ;
    if last then begin
      emitln out "[@@deriving yojson {strict= false}]" ;
      emit_newline out
    end
  end

(* generate union type definition *)
let gen_union_type nsid out name (spec : union_spec) =
  let type_name = Naming.type_name name in
  let is_closed = Option.value spec.closed ~default:false in
  emitln out (Printf.sprintf "type %s =" type_name) ;
  List.iter
    (fun ref_str ->
      let variant_name = Naming.variant_name_of_ref ref_str in
      let payload_type = gen_ref_type nsid out ref_str in
      emitln out (Printf.sprintf "  | %s of %s" variant_name payload_type) )
    spec.refs ;
  if not is_closed then emitln out "  | Unknown of Yojson.Safe.t" ;
  emit_newline out ;
  (* generate of_yojson function *)
  emitln out (Printf.sprintf "let %s_of_yojson json =" type_name) ;
  emitln out "  let open Yojson.Safe.Util in" ;
  emitln out "  try" ;
  emitln out "    match json |> member \"$type\" |> to_string with" ;
  List.iter
    (fun ref_str ->
      let variant_name = Naming.variant_name_of_ref ref_str in
      let full_type_uri = gen_type_uri nsid ref_str in
      let payload_type = gen_ref_type nsid out ref_str in
      emitln out (Printf.sprintf "    | \"%s\" ->" full_type_uri) ;
      emitln out
        (Printf.sprintf "        (match %s_of_yojson json with" payload_type) ;
      emitln out (Printf.sprintf "         | Ok v -> Ok (%s v)" variant_name) ;
      emitln out "         | Error e -> Error e)" )
    spec.refs ;
  if is_closed then emitln out "    | t -> Error (\"unknown union type: \" ^ t)"
  else emitln out "    | _ -> Ok (Unknown json)" ;
  emitln out "  with _ -> Error \"failed to parse union\"" ;
  emit_newline out ;
  (* generate to_yojson function - inject $type field *)
  emitln out (Printf.sprintf "let %s_to_yojson = function" type_name) ;
  List.iter
    (fun ref_str ->
      let variant_name = Naming.variant_name_of_ref ref_str in
      let full_type_uri = gen_type_uri nsid ref_str in
      let payload_type = gen_ref_type nsid out ref_str in
      emitln out (Printf.sprintf "  | %s v ->" variant_name) ;
      emitln out
        (Printf.sprintf "      (match %s_to_yojson v with" payload_type) ;
      emitln out
        (Printf.sprintf
           "       | `Assoc fields -> `Assoc ((\"$type\", `String \"%s\") :: \
            fields)"
           full_type_uri ) ;
      emitln out "       | other -> other)" )
    spec.refs ;
  if not is_closed then emitln out "  | Unknown j -> j" ;
  emit_newline out

let is_json_encoding encoding = encoding = "application/json" || encoding = ""

let is_bytes_encoding encoding =
  encoding <> "" && encoding <> "application/json"

(* generate params type for query/procedure *)
let gen_params_type nsid out (spec : params_spec) =
  let required = Option.value spec.required ~default:[] in
  emitln out "type params =" ;
  emitln out "  {" ;
  List.iter
    (fun (prop_name, (prop : property)) ->
      let ocaml_name = Naming.field_name prop_name in
      let base_type = gen_type_ref nsid out prop.type_def in
      let is_required = List.mem prop_name required in
      let type_str = if is_required then base_type else base_type ^ " option" in
      let key_attr = Naming.key_annotation prop_name ocaml_name in
      let default_attr = if is_required then "" else " [@default None]" in
      emitln out
        (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str key_attr
           default_attr ) )
    spec.properties ;
  emitln out "  }" ;
  emitln out "[@@deriving yojson {strict= false}]" ;
  emit_newline out

(* generate output type for query/procedure *)
let gen_output_type nsid out (body : body_def) =
  match body.schema with
  | Some (Object spec) ->
      (* handle empty objects as unit *)
      if spec.properties = [] then begin
        emitln out "type output = unit" ;
        emitln out "let output_of_yojson _ = Ok ()" ;
        emitln out "let output_to_yojson () = `Assoc []" ;
        emit_newline out
      end
      else begin
        (* generate inline union types first *)
        gen_inline_unions nsid out spec.properties ;
        let required = Option.value spec.required ~default:[] in
        let nullable = Option.value spec.nullable ~default:[] in
        emitln out "type output =" ;
        emitln out "  {" ;
        List.iter
          (fun (prop_name, (prop : property)) ->
            let ocaml_name = Naming.field_name prop_name in
            let base_type = gen_type_ref nsid out prop.type_def in
            let is_required = List.mem prop_name required in
            let is_nullable = List.mem prop_name nullable in
            let type_str =
              if is_required && not is_nullable then base_type
              else base_type ^ " option"
            in
            let key_attr = Naming.key_annotation prop_name ocaml_name in
            let default_attr =
              if is_required && not is_nullable then "" else " [@default None]"
            in
            emitln out
              (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str key_attr
                 default_attr ) )
          spec.properties ;
        emitln out "  }" ;
        emitln out "[@@deriving yojson {strict= false}]" ;
        emit_newline out
      end
  | Some other_type ->
      let type_str = gen_type_ref nsid out other_type in
      emitln out (Printf.sprintf "type output = %s" type_str) ;
      emitln out "[@@deriving yojson {strict= false}]" ;
      emit_newline out
  | None ->
      emitln out "type output = unit" ;
      emitln out "let output_of_yojson _ = Ok ()" ;
      emitln out "let output_to_yojson () = `Null" ;
      emit_newline out

(* generate query module *)
let gen_query nsid out name (spec : query_spec) =
  (* check if output is bytes *)
  let output_is_bytes =
    match spec.output with
    | Some body ->
        is_bytes_encoding body.encoding
    | None ->
        false
  in
  emitln out
    (Printf.sprintf "(** %s *)" (Option.value spec.description ~default:name)) ;
  emitln out (Printf.sprintf "module %s = struct" (Naming.def_module_name name)) ;
  emitln out (Printf.sprintf "  let nsid = \"%s\"" nsid) ;
  emit_newline out ;
  (* generate params type *)
  ( match spec.parameters with
  | Some params when params.properties <> [] ->
      emit out "  " ;
      gen_params_type nsid out params
  | _ ->
      emitln out "  type params = unit" ;
      emitln out "  let params_to_yojson () = `Assoc []" ;
      emit_newline out ) ;
  (* generate output type *)
  ( if output_is_bytes then begin
      emitln out "  (** raw bytes output with content type *)" ;
      emitln out "  type output = string * string" ;
      emit_newline out
    end
    else
      match spec.output with
      | Some body ->
          emit out "  " ;
          gen_output_type nsid out body
      | None ->
          emitln out "  type output = unit" ;
          emitln out "  let output_of_yojson _ = Ok ()" ;
          emit_newline out ) ;
  (* generate call function *)
  emitln out "  let call" ;
  ( match spec.parameters with
  | Some params when params.properties <> [] ->
      let required = Option.value params.required ~default:[] in
      List.iter
        (fun (prop_name, _) ->
          let ocaml_name = Naming.field_name prop_name in
          let is_required = List.mem prop_name required in
          if is_required then emitln out (Printf.sprintf "      ~%s" ocaml_name)
          else emitln out (Printf.sprintf "      ?%s" ocaml_name) )
        params.properties
  | _ ->
      () ) ;
  emitln out "      (client : Hermes.client) : output Lwt.t =" ;
  ( match spec.parameters with
  | Some params when params.properties <> [] ->
      emit out "    let params : params = {" ;
      let fields =
        List.map
          (fun (prop_name, _) -> Naming.field_name prop_name)
          params.properties
      in
      emit out (String.concat "; " fields) ;
      emitln out "} in" ;
      if output_is_bytes then
        emitln out
          "    Hermes.query_bytes client nsid (params_to_yojson params)"
      else
        emitln out
          "    Hermes.query client nsid (params_to_yojson params) \
           output_of_yojson"
  | _ ->
      if output_is_bytes then
        emitln out "    Hermes.query_bytes client nsid (`Assoc [])"
      else
        emitln out "    Hermes.query client nsid (`Assoc []) output_of_yojson"
  ) ;
  emitln out "end" ; emit_newline out

(* generate procedure module *)
let gen_procedure nsid out name (spec : procedure_spec) =
  (* check if input/output are bytes *)
  let input_is_bytes =
    match spec.input with
    | Some body ->
        is_bytes_encoding body.encoding
    | None ->
        false
  in
  let output_is_bytes =
    match spec.output with
    | Some body ->
        is_bytes_encoding body.encoding
    | None ->
        false
  in
  let input_content_type =
    match spec.input with
    | Some body when is_bytes_encoding body.encoding ->
        body.encoding
    | _ ->
        "application/json"
  in
  emitln out
    (Printf.sprintf "(** %s *)" (Option.value spec.description ~default:name)) ;
  emitln out (Printf.sprintf "module %s = struct" (Naming.def_module_name name)) ;
  emitln out (Printf.sprintf "  let nsid = \"%s\"" nsid) ;
  emit_newline out ;
  (* generate params type *)
  ( match spec.parameters with
  | Some params when params.properties <> [] ->
      emit out "  " ;
      gen_params_type nsid out params
  | _ ->
      emitln out "  type params = unit" ;
      emitln out "  let params_to_yojson () = `Assoc []" ;
      emit_newline out ) ;
  (* generate input type; only for json input with schema *)
  ( if not input_is_bytes then
      match spec.input with
      | Some body when body.schema <> None ->
          emit out "  " ;
          ( match body.schema with
          | Some (Object spec) ->
              if spec.properties = [] then begin
                (* empty object input *)
                emitln out "type input = unit" ;
                emitln out "  let input_of_yojson _ = Ok ()" ;
                emitln out "  let input_to_yojson () = `Assoc []"
              end
              else begin
                (* generate inline union types first *)
                gen_inline_unions nsid out spec.properties ;
                let required = Option.value spec.required ~default:[] in
                emitln out "type input =" ;
                emitln out "    {" ;
                List.iter
                  (fun (prop_name, (prop : property)) ->
                    let ocaml_name = Naming.field_name prop_name in
                    let base_type = gen_type_ref nsid out prop.type_def in
                    let is_required = List.mem prop_name required in
                    let type_str =
                      if is_required then base_type else base_type ^ " option"
                    in
                    let key_attr = Naming.key_annotation prop_name ocaml_name in
                    let default_attr =
                      if is_required then "" else " [@default None]"
                    in
                    emitln out
                      (Printf.sprintf "      %s: %s%s%s;" ocaml_name type_str
                         key_attr default_attr ) )
                  spec.properties ;
                emitln out "    }" ;
                emitln out "  [@@deriving yojson {strict= false}]"
              end
          | Some other_type ->
              emitln out
                (Printf.sprintf "type input = %s"
                   (gen_type_ref nsid out other_type) ) ;
              emitln out "  [@@deriving yojson {strict= false}]"
          | None ->
              () ) ;
          emit_newline out
      | _ ->
          () ) ;
  (* generate output type *)
  ( if output_is_bytes then begin
      emitln out "  (** raw bytes output with content type *)" ;
      emitln out "  type output = (string * string) option" ;
      emit_newline out
    end
    else
      match spec.output with
      | Some body ->
          emit out "  " ;
          gen_output_type nsid out body
      | None ->
          emitln out "  type output = unit" ;
          emitln out "  let output_of_yojson _ = Ok ()" ;
          emit_newline out ) ;
  (* generate call function *)
  emitln out "  let call" ;
  (* add labeled arguments for parameters *)
  ( match spec.parameters with
  | Some params when params.properties <> [] ->
      let required = Option.value params.required ~default:[] in
      List.iter
        (fun (prop_name, _) ->
          let ocaml_name = Naming.field_name prop_name in
          let is_required = List.mem prop_name required in
          if is_required then emitln out (Printf.sprintf "      ~%s" ocaml_name)
          else emitln out (Printf.sprintf "      ?%s" ocaml_name) )
        params.properties
  | _ ->
      () ) ;
  (* add labeled arguments for input *)
  ( if input_is_bytes then
      (* for bytes input, take raw string *)
      emitln out "      ?input"
    else
      match spec.input with
      | Some body -> (
        match body.schema with
        | Some (Object obj_spec) ->
            let required = Option.value obj_spec.required ~default:[] in
            List.iter
              (fun (prop_name, _) ->
                let ocaml_name = Naming.field_name prop_name in
                let is_required = List.mem prop_name required in
                if is_required then
                  emitln out (Printf.sprintf "      ~%s" ocaml_name)
                else emitln out (Printf.sprintf "      ?%s" ocaml_name) )
              obj_spec.properties
        | Some _ ->
            (* non-object input, take as single argument *)
            emitln out "      ~input"
        | None ->
            () )
      | None ->
          () ) ;
  emitln out "      (client : Hermes.client) : output Lwt.t =" ;
  (* build params record *)
  ( match spec.parameters with
  | Some params when params.properties <> [] ->
      emit out "    let params = {" ;
      let fields =
        List.map
          (fun (prop_name, _) -> Naming.field_name prop_name)
          params.properties
      in
      emit out (String.concat "; " fields) ;
      emitln out "} in"
  | _ ->
      emitln out "    let params = () in" ) ;
  (* generate the call based on input/output types *)
  if input_is_bytes then begin
    (* bytes input - choose between procedure_blob and procedure_bytes *)
    if output_is_bytes then
      (* bytes-in, bytes-out: use procedure_bytes *)
      emitln out
        (Printf.sprintf
           "    Hermes.procedure_bytes client nsid (params_to_yojson params) \
            input ~content_type:\"%s\""
           input_content_type )
    else if spec.output = None then
      (* bytes-in, no output: use procedure_bytes and map to unit *)
      emitln out
        (Printf.sprintf
           "    let open Lwt.Syntax in\n\
           \    let* _ = Hermes.procedure_bytes client nsid (params_to_yojson \
            params) input ~content_type:\"%s\" in\n\
           \    Lwt.return ()"
           input_content_type )
    else
      (* bytes-in, json-out: use procedure_blob *)
      emitln out
        (Printf.sprintf
           "    Hermes.procedure_blob client nsid (params_to_yojson params) \
            (Bytes.of_string (Option.value input ~default:\"\")) \
            ~content_type:\"%s\" output_of_yojson"
           input_content_type )
  end
  else begin
    (* json input - build input and use procedure *)
    ( match spec.input with
    | Some body -> (
      match body.schema with
      | Some (Object obj_spec) ->
          if obj_spec.properties = [] then
            (* empty object uses unit *)
            emitln out "    let input = Some (input_to_yojson ()) in"
          else begin
            emit out "    let input = Some ({" ;
            let fields =
              List.map
                (fun (prop_name, _) -> Naming.field_name prop_name)
                obj_spec.properties
            in
            emit out (String.concat "; " fields) ;
            emitln out "} |> input_to_yojson) in"
          end
      | Some _ ->
          emitln out "    let input = Some (input_to_yojson input) in"
      | None ->
          emitln out "    let input = None in" )
    | None ->
        emitln out "    let input = None in" ) ;
    emitln out
      "    Hermes.procedure client nsid (params_to_yojson params) input \
       output_of_yojson"
  end ;
  emitln out "end" ;
  emit_newline out

(* generate token constant *)
let gen_token nsid out name (spec : token_spec) =
  let full_uri = nsid ^ "#" ^ name in
  emitln out
    (Printf.sprintf "(** %s *)" (Option.value spec.description ~default:name)) ;
  emitln out (Printf.sprintf "let %s = \"%s\"" (Naming.type_name name) full_uri) ;
  emit_newline out

(* generate string type alias (for strings with knownValues) *)
let gen_string_type out name (spec : string_spec) =
  let type_name = Naming.type_name name in
  emitln out
    (Printf.sprintf "(** string type with known values%s *)"
       (match spec.description with Some d -> ": " ^ d | None -> "") ) ;
  emitln out (Printf.sprintf "type %s = string" type_name) ;
  emitln out (Printf.sprintf "let %s_of_yojson = function" type_name) ;
  emitln out "  | `String s -> Ok s" ;
  emitln out (Printf.sprintf "  | _ -> Error \"%s: expected string\"" type_name) ;
  emitln out (Printf.sprintf "let %s_to_yojson s = `String s" type_name) ;
  emit_newline out

let find_sccs = Scc.find_def_sccs

(* helper to check if a def generates a type (vs token/query/procedure) *)
let is_type_def def =
  match def.type_def with
  | Object _ | Union _ | Record _ ->
      true
  | String spec when spec.known_values <> None ->
      true
  | _ ->
      false

(* helper to check if a def is an object type (can use [@@deriving yojson]) *)
let is_object_def def =
  match def.type_def with Object _ | Record _ -> true | _ -> false

(* generate a single definition *)
let gen_single_def ?(first = true) ?(last = true) nsid out def =
  match def.type_def with
  | Object spec ->
      gen_object_type ~first ~last nsid out def.name spec
  | Union spec ->
      (* unions always generate their own converters, so they're always "complete" *)
      gen_union_type nsid out def.name spec
  | Token spec ->
      gen_token nsid out def.name spec
  | Query spec ->
      gen_query nsid out def.name spec
  | Procedure spec ->
      gen_procedure nsid out def.name spec
  | Record spec ->
      gen_object_type ~first ~last nsid out def.name spec.record
  | String spec when spec.known_values <> None ->
      gen_string_type out def.name spec
  | String _
  | Integer _
  | Boolean _
  | Bytes _
  | Blob _
  | CidLink _
  | Array _
  | Ref _
  | Unknown _
  | Subscription _ ->
      ()

(* generate a group of mutually recursive definitions (SCC) *)
let gen_scc nsid out scc =
  match scc with
  | [] ->
      ()
  | [def] ->
      (* single definition, no cycle *)
      gen_single_def nsid out def
  | defs ->
      (* multiple definitions forming a cycle *)
      (* first, collect and generate all inline unions from all objects in the SCC *)
      List.iter
        (fun def ->
          match def.type_def with
          | Object spec ->
              gen_inline_unions nsid out spec.properties
          | Record spec ->
              gen_inline_unions nsid out spec.record.properties
          | _ ->
              () )
        defs ;
      (* separate object-like types from others *)
      let obj_defs = List.filter is_object_def defs in
      let other_defs = List.filter (fun d -> not (is_object_def d)) defs in
      (* generate other types first (unions, etc.) - they define their own converters *)
      List.iter (fun def -> gen_single_def nsid out def) other_defs ;
      (* generate object types as mutually recursive *)
      let n = List.length obj_defs in
      List.iteri
        (fun i def ->
          let first = i = 0 in
          let last = i = n - 1 in
          match def.type_def with
          | Object spec ->
              (* skip inline unions since we already generated them above *)
              let required = Option.value spec.required ~default:[] in
              let nullable = Option.value spec.nullable ~default:[] in
              let keyword = if first then "type" else "and" in
              if spec.properties = [] then begin
                emitln out
                  (Printf.sprintf "%s %s = unit" keyword
                     (Naming.type_name def.name) ) ;
                if last then begin
                  (* for empty objects in a recursive group, we still need deriving *)
                  emitln out "[@@deriving yojson {strict= false}]" ;
                  emit_newline out
                end
              end
              else begin
                emitln out
                  (Printf.sprintf "%s %s =" keyword (Naming.type_name def.name)) ;
                emitln out "  {" ;
                List.iter
                  (fun (prop_name, (prop : property)) ->
                    let ocaml_name = Naming.field_name prop_name in
                    let base_type = gen_type_ref nsid out prop.type_def in
                    let is_required = List.mem prop_name required in
                    let is_nullable = List.mem prop_name nullable in
                    let type_str =
                      if is_required && not is_nullable then base_type
                      else base_type ^ " option"
                    in
                    let key_attr = Naming.key_annotation prop_name ocaml_name in
                    let default_attr =
                      if is_required && not is_nullable then ""
                      else " [@default None]"
                    in
                    emitln out
                      (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str
                         key_attr default_attr ) )
                  spec.properties ;
                emitln out "  }" ;
                if last then begin
                  emitln out "[@@deriving yojson {strict= false}]" ;
                  emit_newline out
                end
              end
          | Record spec ->
              let obj_spec = spec.record in
              let required = Option.value obj_spec.required ~default:[] in
              let nullable = Option.value obj_spec.nullable ~default:[] in
              let keyword = if first then "type" else "and" in
              if obj_spec.properties = [] then begin
                emitln out
                  (Printf.sprintf "%s %s = unit" keyword
                     (Naming.type_name def.name) ) ;
                if last then begin
                  emitln out "[@@deriving yojson {strict= false}]" ;
                  emit_newline out
                end
              end
              else begin
                emitln out
                  (Printf.sprintf "%s %s =" keyword (Naming.type_name def.name)) ;
                emitln out "  {" ;
                List.iter
                  (fun (prop_name, (prop : property)) ->
                    let ocaml_name = Naming.field_name prop_name in
                    let base_type = gen_type_ref nsid out prop.type_def in
                    let is_required = List.mem prop_name required in
                    let is_nullable = List.mem prop_name nullable in
                    let type_str =
                      if is_required && not is_nullable then base_type
                      else base_type ^ " option"
                    in
                    let key_attr = Naming.key_annotation prop_name ocaml_name in
                    let default_attr =
                      if is_required && not is_nullable then ""
                      else " [@default None]"
                    in
                    emitln out
                      (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str
                         key_attr default_attr ) )
                  obj_spec.properties ;
                emitln out "  }" ;
                if last then begin
                  emitln out "[@@deriving yojson {strict= false}]" ;
                  emit_newline out
                end
              end
          | _ ->
              () )
        obj_defs

(* generate complete lexicon module *)
let gen_lexicon_module (doc : lexicon_doc) : string =
  let out = make_output () in
  let nsid = doc.id in
  (* header *)
  emitln out (Printf.sprintf "(* generated from %s *)" nsid) ;
  emit_newline out ;
  (* find strongly connected components *)
  let sccs = find_sccs nsid doc.defs in
  (* generate each SCC *)
  List.iter (gen_scc nsid out) sccs ;
  Emitter.contents out

(* get all imports needed for a lexicon *)
let get_imports (doc : lexicon_doc) : string list =
  let out = make_output () in
  let nsid = doc.id in
  (* traverse all definitions to collect imports *)
  let rec collect_from_type = function
    | Array {items; _} ->
        collect_from_type items
    | Ref {ref_; _} ->
        let _ = gen_ref_type nsid out ref_ in
        ()
    | Union {refs; _} ->
        List.iter
          (fun r ->
            let _ = gen_ref_type nsid out r in
            () )
          refs
    | Object {properties; _} ->
        List.iter
          (fun (_, (prop : property)) -> collect_from_type prop.type_def)
          properties
    | Query {parameters; output; _} ->
        Option.iter
          (fun p ->
            List.iter
              (fun (_, (prop : property)) -> collect_from_type prop.type_def)
              p.properties )
          parameters ;
        Option.iter (fun o -> Option.iter collect_from_type o.schema) output
    | Procedure {parameters; input; output; _} ->
        Option.iter
          (fun p ->
            List.iter
              (fun (_, (prop : property)) -> collect_from_type prop.type_def)
              p.properties )
          parameters ;
        Option.iter (fun i -> Option.iter collect_from_type i.schema) input ;
        Option.iter (fun o -> Option.iter collect_from_type o.schema) output
    | Record {record; _} ->
        List.iter
          (fun (_, (prop : property)) -> collect_from_type prop.type_def)
          record.properties
    | _ ->
        ()
  in
  List.iter (fun def -> collect_from_type def.type_def) doc.defs ;
  Emitter.get_imports out

(* get external nsid dependencies - delegated to Scc module *)
let get_external_nsids = Scc.get_external_nsids

(* generate a merged lexicon module from multiple lexicons *)
let gen_merged_lexicon_module (docs : lexicon_doc list) : string =
  let out = make_output () in
  (* collect all nsids in this merged group for local ref detection *)
  let merged_nsids = List.map (fun d -> d.id) docs in
  (* header *)
  emitln out
    (Printf.sprintf "(* generated from lexicons: %s *)"
       (String.concat ", " merged_nsids) ) ;
  emit_newline out ;
  (* collect all defs from all docs *)
  let all_defs =
    List.concat_map
      (fun doc -> List.map (fun def -> (doc.id, def)) doc.defs)
      docs
  in
  (* collect all inline unions as pseudo-defs for proper ordering *)
  let rec collect_inline_unions_from_type nsid context acc type_def =
    match type_def with
    | Union spec ->
        (* found an inline union - create pseudo-def entry *)
        let union_name = Naming.type_name context in
        (nsid, union_name, spec.refs, spec) :: acc
    | Array {items; _} ->
        collect_inline_unions_from_type nsid (context ^ "_item") acc items
    | Object {properties; _} ->
        List.fold_left
          (fun a (prop_name, (prop : property)) ->
            collect_inline_unions_from_type nsid prop_name a prop.type_def )
          acc properties
    | _ ->
        acc
  in
  let all_inline_unions =
    List.concat_map
      (fun (nsid, def) ->
        match def.type_def with
        | Object spec ->
            List.fold_left
              (fun acc (prop_name, (prop : property)) ->
                collect_inline_unions_from_type nsid prop_name acc prop.type_def )
              [] spec.properties
        | Record spec ->
            List.fold_left
              (fun acc (prop_name, (prop : property)) ->
                collect_inline_unions_from_type nsid prop_name acc prop.type_def )
              [] spec.record.properties
        | _ ->
            [] )
      all_defs
  in
  (* create a lookup for inline unions by their name *)
  let inline_union_map = Hashtbl.create 64 in
  List.iter
    (fun (nsid, name, refs, spec) ->
      Hashtbl.add inline_union_map
        (nsid ^ "#__inline__" ^ name)
        (nsid, name, refs, spec) )
    all_inline_unions ;
  (* detect inline union name collisions - same name but different refs *)
  let inline_union_name_map = Hashtbl.create 64 in
  List.iter
    (fun (nsid, name, refs, _spec) ->
      let sorted_refs = List.sort String.compare refs in
      let existing = Hashtbl.find_opt inline_union_name_map name in
      match existing with
      | None ->
          Hashtbl.add inline_union_name_map name [(nsid, sorted_refs)]
      | Some entries ->
          (* check if this is a different union (different refs) *)
          if not (List.exists (fun (_, r) -> r = sorted_refs) entries) then
            Hashtbl.replace inline_union_name_map name
              ((nsid, sorted_refs) :: entries) )
    all_inline_unions ;
  let colliding_inline_union_names =
    Hashtbl.fold
      (fun name entries acc ->
        if List.length entries > 1 then name :: acc else acc )
      inline_union_name_map []
  in
  (* the "host" nsid is the first one - types from here keep short names *)
  let host_nsid = List.hd merged_nsids in
  (* function to get unique inline union name *)
  (* only prefix names from "visiting" nsids, not the host *)
  let get_unique_inline_union_name nsid name =
    if List.mem name colliding_inline_union_names && nsid <> host_nsid then
      Naming.flat_name_of_nsid nsid ^ "_" ^ name
    else name
  in
  (* detect name collisions - names that appear in multiple nsids *)
  let name_counts = Hashtbl.create 64 in
  List.iter
    (fun (nsid, def) ->
      let existing = Hashtbl.find_opt name_counts def.name in
      match existing with
      | None ->
          Hashtbl.add name_counts def.name [nsid]
      | Some nsids when not (List.mem nsid nsids) ->
          Hashtbl.replace name_counts def.name (nsid :: nsids)
      | _ ->
          () )
    all_defs ;
  let colliding_names =
    Hashtbl.fold
      (fun name nsids acc -> if List.length nsids > 1 then name :: acc else acc)
      name_counts []
  in
  (* function to get unique type name, adding nsid prefix for collisions *)
  (* only prefix names from "visiting" nsids, not the host *)
  let get_unique_type_name nsid def_name =
    if List.mem def_name colliding_names && nsid <> host_nsid then
      (* use full nsid as prefix to guarantee uniqueness *)
      (* app.bsky.feed.defs#viewerState -> app_bsky_feed_defs_viewer_state *)
      let prefix = Naming.flat_name_of_nsid nsid ^ "_" in
      Naming.type_name (prefix ^ def_name)
    else Naming.type_name def_name
  in
  (* for merged modules, we need to handle refs differently:
     - refs to other nsids in the merged group become local refs
     - refs within same nsid stay as local refs *)
  (* custom ref type generator that treats merged nsids as local *)
  let rec gen_merged_type_ref current_nsid type_def =
    match type_def with
    | String _ ->
        "string"
    | Integer {maximum; _} -> (
      match maximum with Some m when m > 1073741823 -> "int64" | _ -> "int" )
    | Boolean _ ->
        "bool"
    | Bytes _ ->
        "bytes"
    | Blob _ ->
        "Hermes.blob"
    | CidLink _ ->
        "Cid.t"
    | Array {items; _} ->
        let item_type = gen_merged_type_ref current_nsid items in
        item_type ^ " list"
    | Object _ ->
        "object_todo"
    | Ref {ref_; _} ->
        gen_merged_ref_type current_nsid ref_
    | Union {refs; _} -> (
      match lookup_union_name out refs with
      | Some name ->
          name
      | None ->
          gen_union_type_name refs )
    | Token _ ->
        "string"
    | Unknown _ ->
        "Yojson.Safe.t"
    | Query _ | Procedure _ | Subscription _ | Record _ ->
        "unit (* primary type *)"
  and gen_merged_ref_type current_nsid ref_str =
    if String.length ref_str > 0 && ref_str.[0] = '#' then begin
      (* local ref within same nsid *)
      let def_name = String.sub ref_str 1 (String.length ref_str - 1) in
      get_unique_type_name current_nsid def_name
    end
    else begin
      match String.split_on_char '#' ref_str with
      | [ext_nsid; def_name] ->
          if List.mem ext_nsid merged_nsids then
            (* ref to another nsid in the merged group - use unique name *)
            get_unique_type_name ext_nsid def_name
          else begin
            (* truly external ref *)
            let flat_module = Naming.flat_module_name_of_nsid ext_nsid in
            add_import out flat_module ;
            flat_module ^ "." ^ Naming.type_name def_name
          end
      | [ext_nsid] ->
          if List.mem ext_nsid merged_nsids then
            get_unique_type_name ext_nsid "main"
          else begin
            let flat_module = Naming.flat_module_name_of_nsid ext_nsid in
            add_import out flat_module ; flat_module ^ ".main"
          end
      | _ ->
          "invalid_ref"
    end
  in
  (* generate converter expression for reading a type from json *)
  (* returns (converter_expr, needs_result_unwrap) - if needs_result_unwrap is true, caller should apply Result.get_ok *)
  let gen_of_yojson_expr current_nsid type_def =
    match type_def with
    | String _ | Token _ ->
        ("to_string", false)
    | Integer {maximum; _} -> (
      match maximum with
      | Some m when m > 1073741823 ->
          ("(fun j -> Int64.of_int (to_int j))", false)
      | _ ->
          ("to_int", false) )
    | Boolean _ ->
        ("to_bool", false)
    | Bytes _ ->
        ("(fun j -> Bytes.of_string (to_string j))", false)
    | Blob _ ->
        ("Hermes.blob_of_yojson", true)
    | CidLink _ ->
        ("Cid.of_yojson", true)
    | Array {items; _} ->
        let item_type = gen_merged_type_ref current_nsid items in
        ( Printf.sprintf
            "(fun j -> to_list j |> List.filter_map (fun x -> match \
             %s_of_yojson x with Ok v -> Some v | _ -> None))"
            item_type
        , false )
    | Ref {ref_; _} ->
        let type_name = gen_merged_ref_type current_nsid ref_ in
        (type_name ^ "_of_yojson", true)
    | Union {refs; _} ->
        let type_name =
          match lookup_union_name out refs with
          | Some n ->
              n
          | None ->
              gen_union_type_name refs
        in
        (type_name ^ "_of_yojson", true)
    | Unknown _ ->
        ("(fun j -> j)", false)
    | _ ->
        ("(fun _ -> failwith \"unsupported type\")", false)
  in
  (* generate converter expression for writing a type to json *)
  let gen_to_yojson_expr current_nsid type_def =
    match type_def with
    | String _ | Token _ ->
        "(fun s -> `String s)"
    | Integer {maximum; _} -> (
      match maximum with
      | Some m when m > 1073741823 ->
          "(fun i -> `Int (Int64.to_int i))"
      | _ ->
          "(fun i -> `Int i)" )
    | Boolean _ ->
        "(fun b -> `Bool b)"
    | Bytes _ ->
        "(fun b -> `String (Bytes.to_string b))"
    | Blob _ ->
        "Hermes.blob_to_yojson"
    | CidLink _ ->
        "Cid.to_yojson"
    | Array {items; _} ->
        let item_type = gen_merged_type_ref current_nsid items in
        Printf.sprintf "(fun l -> `List (List.map %s_to_yojson l))" item_type
    | Ref {ref_; _} ->
        let type_name = gen_merged_ref_type current_nsid ref_ in
        type_name ^ "_to_yojson"
    | Union {refs; _} ->
        let type_name =
          match lookup_union_name out refs with
          | Some n ->
              n
          | None ->
              gen_union_type_name refs
        in
        type_name ^ "_to_yojson"
    | Unknown _ ->
        "(fun j -> j)"
    | _ ->
        "(fun _ -> `Null)"
  in
  (* generate type uri for merged context *)
  let gen_merged_type_uri current_nsid ref_str =
    if String.length ref_str > 0 && ref_str.[0] = '#' then
      current_nsid ^ ref_str
    else ref_str
  in
  (* register inline union names without generating code *)
  let register_merged_inline_unions nsid properties =
    let rec collect_inline_unions_with_context context acc type_def =
      match type_def with
      | Union spec ->
          (context, spec.refs, spec) :: acc
      | Array {items; _} ->
          collect_inline_unions_with_context (context ^ "_item") acc items
      | _ ->
          acc
    in
    let inline_unions =
      List.fold_left
        (fun acc (prop_name, (prop : property)) ->
          collect_inline_unions_with_context prop_name acc prop.type_def )
        [] properties
    in
    List.iter
      (fun (context, refs, _spec) ->
        let base_name = Naming.type_name context in
        let unique_name = get_unique_inline_union_name nsid base_name in
        register_union_name out refs unique_name )
      inline_unions
  in
  (* generate object type for merged context *)
  let gen_merged_object_type ?(first = true) ?(last = true) current_nsid name
      (spec : object_spec) =
    let required = Option.value spec.required ~default:[] in
    let nullable = Option.value spec.nullable ~default:[] in
    let keyword = if first then "type" else "and" in
    let type_name = get_unique_type_name current_nsid name in
    if spec.properties = [] then begin
      emitln out (Printf.sprintf "%s %s = unit" keyword type_name) ;
      if last then begin
        emitln out (Printf.sprintf "let %s_of_yojson _ = Ok ()" type_name) ;
        emitln out (Printf.sprintf "let %s_to_yojson () = `Assoc []" type_name) ;
        emit_newline out
      end
    end
    else begin
      if first then register_merged_inline_unions current_nsid spec.properties ;
      emitln out (Printf.sprintf "%s %s =" keyword type_name) ;
      emitln out "  {" ;
      List.iter
        (fun (prop_name, (prop : property)) ->
          let ocaml_name = Naming.field_name prop_name in
          let base_type = gen_merged_type_ref current_nsid prop.type_def in
          let is_required = List.mem prop_name required in
          let is_nullable = List.mem prop_name nullable in
          let type_str =
            if is_required && not is_nullable then base_type
            else base_type ^ " option"
          in
          let key_attr = Naming.key_annotation prop_name ocaml_name in
          let default_attr =
            if is_required && not is_nullable then "" else " [@default None]"
          in
          emitln out
            (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str key_attr
               default_attr ) )
        spec.properties ;
      emitln out "  }" ;
      if last then begin
        emitln out "[@@deriving yojson {strict= false}]" ;
        emit_newline out
      end
    end
  in
  (* generate union type for merged context *)
  let gen_merged_union_type current_nsid name (spec : union_spec) =
    let type_name = get_unique_type_name current_nsid name in
    let is_closed = Option.value spec.closed ~default:false in
    emitln out (Printf.sprintf "type %s =" type_name) ;
    List.iter
      (fun ref_str ->
        let variant_name = Naming.variant_name_of_ref ref_str in
        let payload_type = gen_merged_ref_type current_nsid ref_str in
        emitln out (Printf.sprintf "  | %s of %s" variant_name payload_type) )
      spec.refs ;
    if not is_closed then emitln out "  | Unknown of Yojson.Safe.t" ;
    emit_newline out ;
    emitln out (Printf.sprintf "let %s_of_yojson json =" type_name) ;
    emitln out "  let open Yojson.Safe.Util in" ;
    emitln out "  try" ;
    emitln out "    match json |> member \"$type\" |> to_string with" ;
    List.iter
      (fun ref_str ->
        let variant_name = Naming.variant_name_of_ref ref_str in
        let full_type_uri = gen_merged_type_uri current_nsid ref_str in
        let payload_type = gen_merged_ref_type current_nsid ref_str in
        emitln out (Printf.sprintf "    | \"%s\" ->" full_type_uri) ;
        emitln out
          (Printf.sprintf "        (match %s_of_yojson json with" payload_type) ;
        emitln out (Printf.sprintf "         | Ok v -> Ok (%s v)" variant_name) ;
        emitln out "         | Error e -> Error e)" )
      spec.refs ;
    if is_closed then
      emitln out "    | t -> Error (\"unknown union type: \" ^ t)"
    else emitln out "    | _ -> Ok (Unknown json)" ;
    emitln out "  with _ -> Error \"failed to parse union\"" ;
    emit_newline out ;
    emitln out (Printf.sprintf "let %s_to_yojson = function" type_name) ;
    List.iter
      (fun ref_str ->
        let variant_name = Naming.variant_name_of_ref ref_str in
        let full_type_uri = gen_merged_type_uri current_nsid ref_str in
        let payload_type = gen_merged_ref_type current_nsid ref_str in
        emitln out (Printf.sprintf "  | %s v ->" variant_name) ;
        emitln out
          (Printf.sprintf "      (match %s_to_yojson v with" payload_type) ;
        emitln out
          (Printf.sprintf
             "       | `Assoc fields -> `Assoc ((\"$type\", `String \"%s\") :: \
              fields)"
             full_type_uri ) ;
        emitln out "       | other -> other)" )
      spec.refs ;
    if not is_closed then emitln out "  | Unknown j -> j" ;
    emit_newline out
  in
  (* collect refs for merged SCC detection, using compound keys (nsid#name) *)
  let collect_merged_local_refs current_nsid acc type_def =
    let rec aux acc = function
      | Array {items; _} ->
          aux acc items
      | Ref {ref_; _} ->
          if String.length ref_ > 0 && ref_.[0] = '#' then
            (* local ref: #foo -> current_nsid#foo *)
            let def_name = String.sub ref_ 1 (String.length ref_ - 1) in
            (current_nsid ^ "#" ^ def_name) :: acc
          else begin
            match String.split_on_char '#' ref_ with
            | [ext_nsid; def_name] when List.mem ext_nsid merged_nsids ->
                (* cross-nsid ref within merged group *)
                (ext_nsid ^ "#" ^ def_name) :: acc
            | _ ->
                acc
          end
      | Union {refs; _} ->
          List.fold_left
            (fun a r ->
              if String.length r > 0 && r.[0] = '#' then
                let def_name = String.sub r 1 (String.length r - 1) in
                (current_nsid ^ "#" ^ def_name) :: a
              else
                match String.split_on_char '#' r with
                | [ext_nsid; def_name] when List.mem ext_nsid merged_nsids ->
                    (ext_nsid ^ "#" ^ def_name) :: a
                | _ ->
                    a )
            acc refs
      | Object {properties; _} ->
          List.fold_left
            (fun a (_, (prop : property)) -> aux a prop.type_def)
            acc properties
      | Record {record; _} ->
          List.fold_left
            (fun a (_, (prop : property)) -> aux a prop.type_def)
            acc record.properties
      | Query {parameters; output; _} -> (
          let acc =
            match parameters with
            | Some params ->
                List.fold_left
                  (fun a (_, (prop : property)) -> aux a prop.type_def)
                  acc params.properties
            | None ->
                acc
          in
          match output with
          | Some body ->
              Option.fold ~none:acc ~some:(aux acc) body.schema
          | None ->
              acc )
      | Procedure {parameters; input; output; _} -> (
          let acc =
            match parameters with
            | Some params ->
                List.fold_left
                  (fun a (_, (prop : property)) -> aux a prop.type_def)
                  acc params.properties
            | None ->
                acc
          in
          let acc =
            match input with
            | Some body ->
                Option.fold ~none:acc ~some:(aux acc) body.schema
            | None ->
                acc
          in
          match output with
          | Some body ->
              Option.fold ~none:acc ~some:(aux acc) body.schema
          | None ->
              acc )
      | _ ->
          acc
    in
    aux acc type_def
  in
  (* generate merged SCC *)
  let gen_merged_scc scc =
    match scc with
    | [] ->
        ()
    | [(nsid, def)] -> (
      match def.type_def with
      | Object spec ->
          gen_merged_object_type nsid def.name spec
      | Union spec ->
          gen_merged_union_type nsid def.name spec
      | Token spec ->
          gen_token nsid out def.name spec
      | Query spec ->
          gen_query nsid out def.name spec
      | Procedure spec ->
          gen_procedure nsid out def.name spec
      | Record spec ->
          gen_merged_object_type nsid def.name spec.record
      | String spec when spec.known_values <> None ->
          gen_string_type out def.name spec
      | Array {items; _} ->
          (* generate inline union for array items if needed *)
          ( match items with
          | Union spec ->
              let item_type_name = Naming.type_name (def.name ^ "_item") in
              register_union_name out spec.refs item_type_name ;
              gen_merged_union_type nsid (def.name ^ "_item") spec
          | _ ->
              () ) ;
          (* generate type alias for array *)
          let type_name = get_unique_type_name nsid def.name in
          let item_type = gen_merged_type_ref nsid items in
          emitln out (Printf.sprintf "type %s = %s list" type_name item_type) ;
          emitln out (Printf.sprintf "let %s_of_yojson json =" type_name) ;
          emitln out "  let open Yojson.Safe.Util in" ;
          emitln out
            (Printf.sprintf
               "  Ok (to_list json |> List.filter_map (fun x -> match \
                %s_of_yojson x with Ok v -> Some v | _ -> None))"
               item_type ) ;
          emitln out
            (Printf.sprintf
               "let %s_to_yojson l = `List (List.map %s_to_yojson l)" type_name
               item_type ) ;
          emit_newline out
      | _ ->
          () )
    | defs ->
        (* multi-def SCC - register inline union names first *)
        List.iter
          (fun (nsid, def) ->
            match def.type_def with
            | Object spec ->
                register_merged_inline_unions nsid spec.properties
            | Record spec ->
                register_merged_inline_unions nsid spec.record.properties
            | _ ->
                () )
          defs ;
        let obj_defs =
          List.filter
            (fun (_, def) ->
              match def.type_def with Object _ | Record _ -> true | _ -> false )
            defs
        in
        let other_defs =
          List.filter
            (fun (_, def) ->
              match def.type_def with Object _ | Record _ -> false | _ -> true )
            defs
        in
        List.iter
          (fun (nsid, def) ->
            match def.type_def with
            | Union spec ->
                gen_merged_union_type nsid def.name spec
            | Token spec ->
                gen_token nsid out def.name spec
            | Query spec ->
                gen_query nsid out def.name spec
            | Procedure spec ->
                gen_procedure nsid out def.name spec
            | String spec when spec.known_values <> None ->
                gen_string_type out def.name spec
            | _ ->
                () )
          other_defs ;
        let n = List.length obj_defs in
        List.iteri
          (fun i (nsid, def) ->
            let first = i = 0 in
            let last = i = n - 1 in
            match def.type_def with
            | Object spec ->
                let required = Option.value spec.required ~default:[] in
                let nullable = Option.value spec.nullable ~default:[] in
                let keyword = if first then "type" else "and" in
                let type_name = get_unique_type_name nsid def.name in
                if spec.properties = [] then begin
                  emitln out (Printf.sprintf "%s %s = unit" keyword type_name) ;
                  if last then begin
                    emitln out "[@@deriving yojson {strict= false}]" ;
                    emit_newline out
                  end
                end
                else begin
                  emitln out (Printf.sprintf "%s %s =" keyword type_name) ;
                  emitln out "  {" ;
                  List.iter
                    (fun (prop_name, (prop : property)) ->
                      let ocaml_name = Naming.field_name prop_name in
                      let base_type = gen_merged_type_ref nsid prop.type_def in
                      let is_required = List.mem prop_name required in
                      let is_nullable = List.mem prop_name nullable in
                      let type_str =
                        if is_required && not is_nullable then base_type
                        else base_type ^ " option"
                      in
                      let key_attr =
                        Naming.key_annotation prop_name ocaml_name
                      in
                      let default_attr =
                        if is_required && not is_nullable then ""
                        else " [@default None]"
                      in
                      emitln out
                        (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str
                           key_attr default_attr ) )
                    spec.properties ;
                  emitln out "  }" ;
                  if last then begin
                    emitln out "[@@deriving yojson {strict= false}]" ;
                    emit_newline out
                  end
                end
            | Record spec ->
                let obj_spec = spec.record in
                let required = Option.value obj_spec.required ~default:[] in
                let nullable = Option.value obj_spec.nullable ~default:[] in
                let keyword = if first then "type" else "and" in
                let type_name = get_unique_type_name nsid def.name in
                if obj_spec.properties = [] then begin
                  emitln out (Printf.sprintf "%s %s = unit" keyword type_name) ;
                  if last then begin
                    emitln out "[@@deriving yojson {strict= false}]" ;
                    emit_newline out
                  end
                end
                else begin
                  emitln out (Printf.sprintf "%s %s =" keyword type_name) ;
                  emitln out "  {" ;
                  List.iter
                    (fun (prop_name, (prop : property)) ->
                      let ocaml_name = Naming.field_name prop_name in
                      let base_type = gen_merged_type_ref nsid prop.type_def in
                      let is_required = List.mem prop_name required in
                      let is_nullable = List.mem prop_name nullable in
                      let type_str =
                        if is_required && not is_nullable then base_type
                        else base_type ^ " option"
                      in
                      let key_attr =
                        Naming.key_annotation prop_name ocaml_name
                      in
                      let default_attr =
                        if is_required && not is_nullable then ""
                        else " [@default None]"
                      in
                      emitln out
                        (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str
                           key_attr default_attr ) )
                    obj_spec.properties ;
                  emitln out "  }" ;
                  if last then begin
                    emitln out "[@@deriving yojson {strict= false}]" ;
                    emit_newline out
                  end
                end
            | _ ->
                () )
          obj_defs
  in
  (* create extended defs that include inline unions as pseudo-entries *)
  (* inline union key format: nsid#__inline__name *)
  let inline_union_defs =
    List.map
      (fun (nsid, name, refs, spec) ->
        let key = nsid ^ "#__inline__" ^ name in
        (* inline unions depend on the types they reference *)
        let deps =
          List.filter_map
            (fun r ->
              if String.length r > 0 && r.[0] = '#' then
                let def_name = String.sub r 1 (String.length r - 1) in
                Some (nsid ^ "#" ^ def_name)
              else
                match String.split_on_char '#' r with
                | [ext_nsid; def_name] when List.mem ext_nsid merged_nsids ->
                    Some (ext_nsid ^ "#" ^ def_name)
                | _ ->
                    None )
            refs
        in
        (key, deps, `InlineUnion (nsid, name, refs, spec)) )
      all_inline_unions
  in
  (* create regular def entries *)
  let regular_def_entries =
    List.map
      (fun (nsid, def) ->
        let key = nsid ^ "#" ^ def.name in
        let base_deps = collect_merged_local_refs nsid [] def.type_def in
        (* add dependencies on inline unions used by this def *)
        let inline_deps =
          match def.type_def with
          | Object spec | Record {record= spec; _} ->
              let rec collect_inline_union_deps acc type_def =
                match type_def with
                | Union _ -> (
                  (* this property uses an inline union - find its name *)
                  match lookup_union_name out [] with
                  | _ ->
                      acc (* we'll handle this differently *) )
                | Array {items; _} ->
                    collect_inline_union_deps acc items
                | _ ->
                    acc
              in
              List.fold_left
                (fun acc (prop_name, (prop : property)) ->
                  match prop.type_def with
                  | Union _ ->
                      let union_name = Naming.type_name prop_name in
                      (nsid ^ "#__inline__" ^ union_name) :: acc
                  | Array {items= Union _; _} ->
                      let union_name = Naming.type_name (prop_name ^ "_item") in
                      (nsid ^ "#__inline__" ^ union_name) :: acc
                  | _ ->
                      collect_inline_union_deps acc prop.type_def )
                [] spec.properties
          | _ ->
              []
        in
        (key, base_deps @ inline_deps, `RegularDef (nsid, def)) )
      all_defs
  in
  (* combine all entries *)
  let all_entries = regular_def_entries @ inline_union_defs in
  (* build dependency map *)
  let deps_map = List.map (fun (k, deps, _) -> (k, deps)) all_entries in
  let entry_map = List.map (fun (k, _, entry) -> (k, entry)) all_entries in
  let all_keys = List.map (fun (k, _, _) -> k) all_entries in
  (* run Tarjan's algorithm on combined entries *)
  let index_counter = ref 0 in
  let indices = Hashtbl.create 64 in
  let lowlinks = Hashtbl.create 64 in
  let on_stack = Hashtbl.create 64 in
  let stack = ref [] in
  let sccs = ref [] in
  let rec strongconnect key =
    let index = !index_counter in
    incr index_counter ;
    Hashtbl.add indices key index ;
    Hashtbl.add lowlinks key index ;
    Hashtbl.add on_stack key true ;
    stack := key :: !stack ;
    let successors =
      try List.assoc key deps_map |> List.filter (fun k -> List.mem k all_keys)
      with Not_found -> []
    in
    List.iter
      (fun succ ->
        if not (Hashtbl.mem indices succ) then begin
          strongconnect succ ;
          Hashtbl.replace lowlinks key
            (min (Hashtbl.find lowlinks key) (Hashtbl.find lowlinks succ))
        end
        else if Hashtbl.find_opt on_stack succ = Some true then
          Hashtbl.replace lowlinks key
            (min (Hashtbl.find lowlinks key) (Hashtbl.find indices succ)) )
      successors ;
    if Hashtbl.find lowlinks key = Hashtbl.find indices key then begin
      let rec pop_scc acc =
        match !stack with
        | [] ->
            acc
        | top :: rest ->
            stack := rest ;
            Hashtbl.replace on_stack top false ;
            if top = key then top :: acc else pop_scc (top :: acc)
      in
      let scc_keys = pop_scc [] in
      let scc_entries =
        List.filter_map (fun k -> List.assoc_opt k entry_map) scc_keys
      in
      if scc_entries <> [] then sccs := scc_entries :: !sccs
    end
  in
  List.iter
    (fun key -> if not (Hashtbl.mem indices key) then strongconnect key)
    all_keys ;
  let ordered_sccs = List.rev !sccs in
  (* helper to generate object type definition only (no converters) *)
  let gen_object_type_only ?(keyword = "type") nsid name (spec : object_spec) =
    let required = Option.value spec.required ~default:[] in
    let nullable = Option.value spec.nullable ~default:[] in
    let type_name = get_unique_type_name nsid name in
    if spec.properties = [] then
      emitln out (Printf.sprintf "%s %s = unit" keyword type_name)
    else begin
      emitln out (Printf.sprintf "%s %s = {" keyword type_name) ;
      List.iter
        (fun (prop_name, (prop : property)) ->
          let ocaml_name = Naming.field_name prop_name in
          let base_type = gen_merged_type_ref nsid prop.type_def in
          let is_required = List.mem prop_name required in
          let is_nullable = List.mem prop_name nullable in
          let type_str =
            if is_required && not is_nullable then base_type
            else base_type ^ " option"
          in
          let key_attr = Naming.key_annotation prop_name ocaml_name in
          let default_attr =
            if is_required && not is_nullable then "" else " [@default None]"
          in
          emitln out
            (Printf.sprintf "  %s: %s%s%s;" ocaml_name type_str key_attr
               default_attr ) )
        spec.properties ;
      emitln out "}"
    end
  in
  (* helper to generate inline union type definition only (no converters) *)
  let gen_inline_union_type_only ?(keyword = "type") nsid name refs spec =
    let is_closed = Option.value spec.closed ~default:false in
    emitln out (Printf.sprintf "%s %s =" keyword name) ;
    List.iter
      (fun ref_str ->
        let variant_name = Naming.qualified_variant_name_of_ref ref_str in
        let payload_type = gen_merged_ref_type nsid ref_str in
        emitln out (Printf.sprintf "  | %s of %s" variant_name payload_type) )
      refs ;
    if not is_closed then emitln out "  | Unknown of Yojson.Safe.t"
  in
  (* helper to generate object converters *)
  let gen_object_converters ?(of_keyword = "let") ?(to_keyword = "let") nsid
      name (spec : object_spec) =
    let required = Option.value spec.required ~default:[] in
    let nullable = Option.value spec.nullable ~default:[] in
    let type_name = get_unique_type_name nsid name in
    if spec.properties = [] then begin
      if of_keyword <> "SKIP" then
        emitln out
          (Printf.sprintf "%s %s_of_yojson _ = Ok ()" of_keyword type_name) ;
      if to_keyword <> "SKIP" then
        emitln out
          (Printf.sprintf "%s %s_to_yojson () = `Assoc []" to_keyword type_name)
    end
    else begin
      (* of_yojson *)
      if of_keyword <> "SKIP" then begin
        emitln out
          (Printf.sprintf "%s %s_of_yojson json =" of_keyword type_name) ;
        emitln out "  let open Yojson.Safe.Util in" ;
        emitln out "  try" ;
        List.iter
          (fun (prop_name, (prop : property)) ->
            let ocaml_name = Naming.field_name prop_name in
            let conv_expr, needs_unwrap =
              gen_of_yojson_expr nsid prop.type_def
            in
            let is_required = List.mem prop_name required in
            let is_nullable = List.mem prop_name nullable in
            let is_optional = (not is_required) || is_nullable in
            if is_optional then begin
              if needs_unwrap then
                emitln out
                  (Printf.sprintf
                     "    let %s = json |> member \"%s\" |> to_option (fun x \
                      -> match %s x with Ok v -> Some v | _ -> None) |> \
                      Option.join in"
                     ocaml_name prop_name conv_expr )
              else
                emitln out
                  (Printf.sprintf
                     "    let %s = json |> member \"%s\" |> to_option %s in"
                     ocaml_name prop_name conv_expr )
            end
            else begin
              if needs_unwrap then
                emitln out
                  (Printf.sprintf
                     "    let %s = json |> member \"%s\" |> %s |> \
                      Result.get_ok in"
                     ocaml_name prop_name conv_expr )
              else
                emitln out
                  (Printf.sprintf "    let %s = json |> member \"%s\" |> %s in"
                     ocaml_name prop_name conv_expr )
            end )
          spec.properties ;
        emit out "    Ok { " ;
        emit out
          (String.concat "; "
             (List.map (fun (pn, _) -> Naming.field_name pn) spec.properties) ) ;
        emitln out " }" ;
        emitln out "  with e -> Error (Printexc.to_string e)" ;
        emit_newline out
      end ;
      (* to_yojson *)
      if to_keyword <> "SKIP" then begin
        emitln out
          (Printf.sprintf "%s %s_to_yojson (r : %s) =" to_keyword type_name
             type_name ) ;
        emitln out "  `Assoc [" ;
        List.iteri
          (fun i (prop_name, (prop : property)) ->
            let ocaml_name = Naming.field_name prop_name in
            let conv_expr = gen_to_yojson_expr nsid prop.type_def in
            let is_required = List.mem prop_name required in
            let is_nullable = List.mem prop_name nullable in
            let is_optional = (not is_required) || is_nullable in
            let comma =
              if i < List.length spec.properties - 1 then ";" else ""
            in
            if is_optional then
              emitln out
                (Printf.sprintf
                   "    (\"%s\", match r.%s with Some v -> %s v | None -> \
                    `Null)%s"
                   prop_name ocaml_name conv_expr comma )
            else
              emitln out
                (Printf.sprintf "    (\"%s\", %s r.%s)%s" prop_name conv_expr
                   ocaml_name comma ) )
          spec.properties ;
        emitln out "  ]" ;
        emit_newline out
      end
    end
  in
  (* helper to generate inline union converters *)
  let gen_inline_union_converters ?(of_keyword = "let") ?(to_keyword = "let")
      nsid name refs spec =
    let is_closed = Option.value spec.closed ~default:false in
    (* of_yojson *)
    if of_keyword <> "SKIP" then begin
      emitln out (Printf.sprintf "%s %s_of_yojson json =" of_keyword name) ;
      emitln out "  let open Yojson.Safe.Util in" ;
      emitln out "  try" ;
      emitln out "    match json |> member \"$type\" |> to_string with" ;
      List.iter
        (fun ref_str ->
          let variant_name = Naming.qualified_variant_name_of_ref ref_str in
          let full_type_uri = gen_merged_type_uri nsid ref_str in
          let payload_type = gen_merged_ref_type nsid ref_str in
          emitln out (Printf.sprintf "    | \"%s\" ->" full_type_uri) ;
          emitln out
            (Printf.sprintf "        (match %s_of_yojson json with" payload_type) ;
          emitln out
            (Printf.sprintf "         | Ok v -> Ok (%s v)" variant_name) ;
          emitln out "         | Error e -> Error e)" )
        refs ;
      if is_closed then
        emitln out "    | t -> Error (\"unknown union type: \" ^ t)"
      else emitln out "    | _ -> Ok (Unknown json)" ;
      emitln out "  with _ -> Error \"failed to parse union\"" ;
      emit_newline out
    end ;
    (* to_yojson *)
    if to_keyword <> "SKIP" then begin
      emitln out (Printf.sprintf "%s %s_to_yojson = function" to_keyword name) ;
      List.iter
        (fun ref_str ->
          let variant_name = Naming.qualified_variant_name_of_ref ref_str in
          let full_type_uri = gen_merged_type_uri nsid ref_str in
          let payload_type = gen_merged_ref_type nsid ref_str in
          emitln out (Printf.sprintf "  | %s v ->" variant_name) ;
          emitln out
            (Printf.sprintf "      (match %s_to_yojson v with" payload_type) ;
          emitln out
            (Printf.sprintf
               "       | `Assoc fields -> `Assoc ((\"$type\", `String \"%s\") \
                :: fields)"
               full_type_uri ) ;
          emitln out "       | other -> other)" )
        refs ;
      if not is_closed then emitln out "  | Unknown j -> j" ;
      emit_newline out
    end
  in
  (* generate each SCC *)
  List.iter
    (fun scc ->
      (* separate inline unions from regular defs *)
      let inline_unions_in_scc =
        List.filter_map (function `InlineUnion x -> Some x | _ -> None) scc
      in
      let regular_defs_in_scc =
        List.filter_map (function `RegularDef x -> Some x | _ -> None) scc
      in
      if inline_unions_in_scc = [] then begin
        (* no inline unions - use standard generation with [@@deriving yojson] *)
        if regular_defs_in_scc <> [] then gen_merged_scc regular_defs_in_scc
      end
      else begin
        (* has inline unions - generate all types first, then all converters *)
        (* register inline union names *)
        List.iter
          (fun (nsid, name, refs, _spec) ->
            let unique_name = get_unique_inline_union_name nsid name in
            register_union_name out refs unique_name ;
            mark_union_generated out unique_name )
          inline_unions_in_scc ;
        (* collect all items to generate *)
        let all_items =
          List.map (fun x -> `Inline x) inline_unions_in_scc
          @ List.map (fun x -> `Regular x) regular_defs_in_scc
        in
        let n = List.length all_items in
        if n = 1 then begin
          (* single item - generate normally *)
          match List.hd all_items with
          | `Inline (nsid, name, refs, spec) ->
              let unique_name = get_unique_inline_union_name nsid name in
              gen_inline_union_type_only nsid unique_name refs spec ;
              emit_newline out ;
              gen_inline_union_converters nsid unique_name refs spec
          | `Regular (nsid, def) -> (
            match def.type_def with
            | Object spec ->
                register_merged_inline_unions nsid spec.properties ;
                gen_object_type_only nsid def.name spec ;
                emit_newline out ;
                gen_object_converters nsid def.name spec
            | Record rspec ->
                register_merged_inline_unions nsid rspec.record.properties ;
                gen_object_type_only nsid def.name rspec.record ;
                emit_newline out ;
                gen_object_converters nsid def.name rspec.record
            | _ ->
                gen_merged_scc [(nsid, def)] )
        end
        else begin
          (* multiple items - generate as mutually recursive types *)
          (* first pass: register inline unions from objects *)
          List.iter
            (function
              | `Regular (nsid, def) -> (
                match def.type_def with
                | Object spec ->
                    register_merged_inline_unions nsid spec.properties
                | Record rspec ->
                    register_merged_inline_unions nsid rspec.record.properties
                | _ ->
                    () )
              | `Inline _ ->
                  () )
            all_items ;
          (* second pass: generate all type definitions *)
          List.iteri
            (fun i item ->
              let keyword = if i = 0 then "type" else "and" in
              match item with
              | `Inline (nsid, name, refs, spec) ->
                  let unique_name = get_unique_inline_union_name nsid name in
                  gen_inline_union_type_only ~keyword nsid unique_name refs spec
              | `Regular (nsid, def) -> (
                match def.type_def with
                | Object spec ->
                    gen_object_type_only ~keyword nsid def.name spec
                | Record rspec ->
                    gen_object_type_only ~keyword nsid def.name rspec.record
                | _ ->
                    () ) )
            all_items ;
          emit_newline out ;
          (* third pass: generate all _of_yojson converters as mutually recursive *)
          List.iteri
            (fun i item ->
              let of_keyword = if i = 0 then "let rec" else "and" in
              match item with
              | `Inline (nsid, name, refs, spec) ->
                  let unique_name = get_unique_inline_union_name nsid name in
                  gen_inline_union_converters ~of_keyword ~to_keyword:"SKIP"
                    nsid unique_name refs spec
              | `Regular (nsid, def) -> (
                match def.type_def with
                | Object spec ->
                    gen_object_converters ~of_keyword ~to_keyword:"SKIP" nsid
                      def.name spec
                | Record rspec ->
                    gen_object_converters ~of_keyword ~to_keyword:"SKIP" nsid
                      def.name rspec.record
                | _ ->
                    () ) )
            all_items ;
          (* fourth pass: generate all _to_yojson converters as mutually recursive *)
          List.iteri
            (fun i item ->
              let to_keyword = if i = 0 then "and" else "and" in
              match item with
              | `Inline (nsid, name, refs, spec) ->
                  let unique_name = get_unique_inline_union_name nsid name in
                  gen_inline_union_converters ~of_keyword:"SKIP" ~to_keyword
                    nsid unique_name refs spec
              | `Regular (nsid, def) -> (
                match def.type_def with
                | Object spec ->
                    gen_object_converters ~of_keyword:"SKIP" ~to_keyword nsid
                      def.name spec
                | Record rspec ->
                    gen_object_converters ~of_keyword:"SKIP" ~to_keyword nsid
                      def.name rspec.record
                | _ ->
                    () ) )
            all_items
        end
      end )
    ordered_sccs ;
  Emitter.contents out

(* generate a re-export stub that selectively exports types from a merged module *)
let gen_reexport_stub ~merged_module_name ~all_merged_docs (doc : lexicon_doc) :
    string =
  let buf = Buffer.create 1024 in
  let emit s = Buffer.add_string buf s in
  let emitln s = Buffer.add_string buf s ; Buffer.add_char buf '\n' in
  (* detect collisions across all merged docs *)
  let all_defs =
    List.concat_map
      (fun d -> List.map (fun def -> (d.id, def)) d.defs)
      all_merged_docs
  in
  let name_counts = Hashtbl.create 64 in
  List.iter
    (fun (nsid, def) ->
      let existing = Hashtbl.find_opt name_counts def.name in
      match existing with
      | None ->
          Hashtbl.add name_counts def.name [nsid]
      | Some nsids when not (List.mem nsid nsids) ->
          Hashtbl.replace name_counts def.name (nsid :: nsids)
      | _ ->
          () )
    all_defs ;
  let colliding_names =
    Hashtbl.fold
      (fun name nsids acc -> if List.length nsids > 1 then name :: acc else acc)
      name_counts []
  in
  (* the "host" nsid is the first one - types from here keep short names *)
  let host_nsid = (List.hd all_merged_docs).id in
  let get_unique_type_name nsid def_name =
    if List.mem def_name colliding_names && nsid <> host_nsid then
      let prefix = Naming.flat_name_of_nsid nsid ^ "_" in
      Naming.type_name (prefix ^ def_name)
    else Naming.type_name def_name
  in
  emitln (Printf.sprintf "(* re-exported from %s *)" merged_module_name) ;
  emitln "" ;
  List.iter
    (fun def ->
      let local_type_name = Naming.type_name def.name in
      let merged_type_name = get_unique_type_name doc.id def.name in
      match def.type_def with
      | Object _ | Record _ | Union _ ->
          (* type alias and converter aliases *)
          emitln
            (Printf.sprintf "type %s = %s.%s" local_type_name merged_module_name
               merged_type_name ) ;
          emitln
            (Printf.sprintf "let %s_of_yojson = %s.%s_of_yojson" local_type_name
               merged_module_name merged_type_name ) ;
          emitln
            (Printf.sprintf "let %s_to_yojson = %s.%s_to_yojson" local_type_name
               merged_module_name merged_type_name ) ;
          emit "\n"
      | String spec when spec.known_values <> None ->
          emitln
            (Printf.sprintf "type %s = %s.%s" local_type_name merged_module_name
               merged_type_name ) ;
          emitln
            (Printf.sprintf "let %s_of_yojson = %s.%s_of_yojson" local_type_name
               merged_module_name merged_type_name ) ;
          emitln
            (Printf.sprintf "let %s_to_yojson = %s.%s_to_yojson" local_type_name
               merged_module_name merged_type_name ) ;
          emit "\n"
      | Array _ ->
          (* re-export array type alias and converters *)
          emitln
            (Printf.sprintf "type %s = %s.%s" local_type_name merged_module_name
               merged_type_name ) ;
          emitln
            (Printf.sprintf "let %s_of_yojson = %s.%s_of_yojson" local_type_name
               merged_module_name merged_type_name ) ;
          emitln
            (Printf.sprintf "let %s_to_yojson = %s.%s_to_yojson" local_type_name
               merged_module_name merged_type_name ) ;
          emit "\n"
      | Token _ ->
          emitln
            (Printf.sprintf "let %s = %s.%s" local_type_name merged_module_name
               merged_type_name ) ;
          emit "\n"
      | Query _ | Procedure _ ->
          let mod_name = Naming.def_module_name def.name in
          emitln
            (Printf.sprintf "module %s = %s.%s" mod_name merged_module_name
               mod_name ) ;
          emit "\n"
      | _ ->
          () )
    doc.defs ;
  Buffer.contents buf

(* generate a shared module for mutually recursive lexicons *)
(* uses Naming.shared_type_name for context-based naming instead of full nsid prefix *)
let gen_shared_module (docs : lexicon_doc list) : string =
  let out = make_output () in
  (* collect all nsids in this shared group *)
  let shared_nsids = List.map (fun d -> d.id) docs in
  (* header *)
  emitln out
    (Printf.sprintf "(* shared module for lexicons: %s *)"
       (String.concat ", " shared_nsids) ) ;
  emit_newline out ;
  (* collect all defs from all docs *)
  let all_defs =
    List.concat_map
      (fun doc -> List.map (fun def -> (doc.id, def)) doc.defs)
      docs
  in
  (* detect name collisions - names that appear in multiple nsids *)
  let name_counts = Hashtbl.create 64 in
  List.iter
    (fun (nsid, def) ->
      let existing = Hashtbl.find_opt name_counts def.name in
      match existing with
      | None ->
          Hashtbl.add name_counts def.name [nsid]
      | Some nsids when not (List.mem nsid nsids) ->
          Hashtbl.replace name_counts def.name (nsid :: nsids)
      | _ ->
          () )
    all_defs ;
  let colliding_names =
    Hashtbl.fold
      (fun name nsids acc -> if List.length nsids > 1 then name :: acc else acc)
      name_counts []
  in
  (* also detect inline union name collisions *)
  let rec collect_inline_union_contexts nsid context acc type_def =
    match type_def with
    | Union spec ->
        (nsid, context, spec.refs) :: acc
    | Array {items; _} ->
        collect_inline_union_contexts nsid (context ^ "_item") acc items
    | Object {properties; _} ->
        List.fold_left
          (fun a (prop_name, (prop : property)) ->
            collect_inline_union_contexts nsid prop_name a prop.type_def )
          acc properties
    | _ ->
        acc
  in
  let all_inline_union_contexts =
    List.concat_map
      (fun (nsid, def) ->
        match def.type_def with
        | Object spec ->
            List.fold_left
              (fun acc (prop_name, (prop : property)) ->
                collect_inline_union_contexts nsid prop_name acc prop.type_def )
              [] spec.properties
        | Record rspec ->
            List.fold_left
              (fun acc (prop_name, (prop : property)) ->
                collect_inline_union_contexts nsid prop_name acc prop.type_def )
              [] rspec.record.properties
        | _ ->
            [] )
      all_defs
  in
  (* group inline unions by context name *)
  let inline_union_by_context = Hashtbl.create 64 in
  List.iter
    (fun (nsid, context, refs) ->
      let key = Naming.type_name context in
      let sorted_refs = List.sort String.compare refs in
      let existing = Hashtbl.find_opt inline_union_by_context key in
      match existing with
      | None ->
          Hashtbl.add inline_union_by_context key [(nsid, sorted_refs)]
      | Some entries ->
          (* collision if different nsid OR different refs *)
          if
            not
              (List.exists (fun (n, r) -> n = nsid && r = sorted_refs) entries)
          then
            Hashtbl.replace inline_union_by_context key
              ((nsid, sorted_refs) :: entries) )
    all_inline_union_contexts ;
  (* add inline union collisions to colliding_names *)
  let colliding_names =
    Hashtbl.fold
      (fun name entries acc ->
        (* collision if more than one entry (different nsid or different refs) *)
        if List.length entries > 1 then name :: acc else acc )
      inline_union_by_context colliding_names
  in
  (* function to get unique type name using shared_type_name for collisions *)
  let get_shared_type_name nsid def_name =
    if List.mem def_name colliding_names then
      (* use context-based name: e.g., feed_viewer_state *)
      Naming.shared_type_name nsid def_name
    else
      (* no collision, use simple name *)
      Naming.type_name def_name
  in
  (* custom ref type generator that treats shared nsids as local *)
  let rec gen_shared_type_ref current_nsid type_def =
    match type_def with
    | String _ ->
        "string"
    | Integer {maximum; _} -> (
      match maximum with Some m when m > 1073741823 -> "int64" | _ -> "int" )
    | Boolean _ ->
        "bool"
    | Bytes _ ->
        "bytes"
    | Blob _ ->
        "Hermes.blob"
    | CidLink _ ->
        "Cid.t"
    | Array {items; _} ->
        let item_type = gen_shared_type_ref current_nsid items in
        item_type ^ " list"
    | Object _ ->
        "object_todo"
    | Ref {ref_; _} ->
        gen_shared_ref_type current_nsid ref_
    | Union {refs; _} -> (
      match lookup_union_name out refs with
      | Some name ->
          name
      | None ->
          gen_union_type_name refs )
    | Token _ ->
        "string"
    | Unknown _ ->
        "Yojson.Safe.t"
    | Query _ | Procedure _ | Subscription _ | Record _ ->
        "unit (* primary type *)"
  and gen_shared_ref_type current_nsid ref_str =
    if String.length ref_str > 0 && ref_str.[0] = '#' then begin
      (* local ref within same nsid *)
      let def_name = String.sub ref_str 1 (String.length ref_str - 1) in
      get_shared_type_name current_nsid def_name
    end
    else begin
      match String.split_on_char '#' ref_str with
      | [ext_nsid; def_name] ->
          if List.mem ext_nsid shared_nsids then
            (* ref to another nsid in the shared group *)
            get_shared_type_name ext_nsid def_name
          else begin
            (* truly external ref *)
            let flat_module = Naming.flat_module_name_of_nsid ext_nsid in
            add_import out flat_module ;
            flat_module ^ "." ^ Naming.type_name def_name
          end
      | [ext_nsid] ->
          if List.mem ext_nsid shared_nsids then
            get_shared_type_name ext_nsid "main"
          else begin
            let flat_module = Naming.flat_module_name_of_nsid ext_nsid in
            add_import out flat_module ; flat_module ^ ".main"
          end
      | _ ->
          "invalid_ref"
    end
  in
  (* generate type uri for shared context *)
  let gen_shared_type_uri current_nsid ref_str =
    if String.length ref_str > 0 && ref_str.[0] = '#' then
      current_nsid ^ ref_str
    else ref_str
  in
  (* generate converter expression for reading a type from json *)
  let gen_shared_of_yojson_expr current_nsid type_def =
    match type_def with
    | String _ | Token _ ->
        ("to_string", false)
    | Integer {maximum; _} -> (
      match maximum with
      | Some m when m > 1073741823 ->
          ("(fun j -> Int64.of_int (to_int j))", false)
      | _ ->
          ("to_int", false) )
    | Boolean _ ->
        ("to_bool", false)
    | Bytes _ ->
        ("(fun j -> Bytes.of_string (to_string j))", false)
    | Blob _ ->
        ("Hermes.blob_of_yojson", true)
    | CidLink _ ->
        ("Cid.of_yojson", true)
    | Array {items; _} ->
        let item_type = gen_shared_type_ref current_nsid items in
        ( Printf.sprintf
            "(fun j -> to_list j |> List.filter_map (fun x -> match \
             %s_of_yojson x with Ok v -> Some v | _ -> None))"
            item_type
        , false )
    | Ref {ref_; _} ->
        let type_name = gen_shared_ref_type current_nsid ref_ in
        (type_name ^ "_of_yojson", true)
    | Union {refs; _} ->
        let type_name =
          match lookup_union_name out refs with
          | Some n ->
              n
          | None ->
              gen_union_type_name refs
        in
        (type_name ^ "_of_yojson", true)
    | Unknown _ ->
        ("(fun j -> j)", false)
    | _ ->
        ("(fun _ -> failwith \"unsupported type\")", false)
  in
  (* generate converter expression for writing a type to json *)
  let gen_shared_to_yojson_expr current_nsid type_def =
    match type_def with
    | String _ | Token _ ->
        "(fun s -> `String s)"
    | Integer {maximum; _} -> (
      match maximum with
      | Some m when m > 1073741823 ->
          "(fun i -> `Int (Int64.to_int i))"
      | _ ->
          "(fun i -> `Int i)" )
    | Boolean _ ->
        "(fun b -> `Bool b)"
    | Bytes _ ->
        "(fun b -> `String (Bytes.to_string b))"
    | Blob _ ->
        "Hermes.blob_to_yojson"
    | CidLink _ ->
        "Cid.to_yojson"
    | Array {items; _} ->
        let item_type = gen_shared_type_ref current_nsid items in
        Printf.sprintf "(fun l -> `List (List.map %s_to_yojson l))" item_type
    | Ref {ref_; _} ->
        let type_name = gen_shared_ref_type current_nsid ref_ in
        type_name ^ "_to_yojson"
    | Union {refs; _} ->
        let type_name =
          match lookup_union_name out refs with
          | Some n ->
              n
          | None ->
              gen_union_type_name refs
        in
        type_name ^ "_to_yojson"
    | Unknown _ ->
        "(fun j -> j)"
    | _ ->
        "(fun _ -> `Null)"
  in
  (* collect inline unions with context-based naming *)
  let get_shared_inline_union_name nsid context =
    let base_name = Naming.type_name context in
    (* check if there's a collision with this inline union name *)
    if List.mem base_name colliding_names then
      Naming.shared_type_name nsid context
    else base_name
  in
  let register_shared_inline_unions nsid properties =
    let rec collect_inline_unions_with_context context acc type_def =
      match type_def with
      | Union spec ->
          (context, spec.refs, spec) :: acc
      | Array {items; _} ->
          collect_inline_unions_with_context (context ^ "_item") acc items
      | _ ->
          acc
    in
    let inline_unions =
      List.fold_left
        (fun acc (prop_name, (prop : property)) ->
          collect_inline_unions_with_context prop_name acc prop.type_def )
        [] properties
    in
    List.iter
      (fun (context, refs, _spec) ->
        let unique_name = get_shared_inline_union_name nsid context in
        register_union_name out refs unique_name )
      inline_unions
  in
  (* generate object type for shared context *)
  let gen_shared_object_type ?(first = true) ?(last = true) current_nsid name
      (spec : object_spec) =
    let required = Option.value spec.required ~default:[] in
    let nullable = Option.value spec.nullable ~default:[] in
    let keyword = if first then "type" else "and" in
    let type_name = get_shared_type_name current_nsid name in
    if spec.properties = [] then begin
      emitln out (Printf.sprintf "%s %s = unit" keyword type_name) ;
      if last then begin
        emitln out (Printf.sprintf "let %s_of_yojson _ = Ok ()" type_name) ;
        emitln out (Printf.sprintf "let %s_to_yojson () = `Assoc []" type_name) ;
        emit_newline out
      end
    end
    else begin
      if first then register_shared_inline_unions current_nsid spec.properties ;
      emitln out (Printf.sprintf "%s %s =" keyword type_name) ;
      emitln out "  {" ;
      List.iter
        (fun (prop_name, (prop : property)) ->
          let ocaml_name = Naming.field_name prop_name in
          let base_type = gen_shared_type_ref current_nsid prop.type_def in
          let is_required = List.mem prop_name required in
          let is_nullable = List.mem prop_name nullable in
          let type_str =
            if is_required && not is_nullable then base_type
            else base_type ^ " option"
          in
          let key_attr = Naming.key_annotation prop_name ocaml_name in
          let default_attr =
            if is_required && not is_nullable then "" else " [@default None]"
          in
          emitln out
            (Printf.sprintf "    %s: %s%s%s;" ocaml_name type_str key_attr
               default_attr ) )
        spec.properties ;
      emitln out "  }" ;
      if last then begin
        emitln out "[@@deriving yojson {strict= false}]" ;
        emit_newline out
      end
    end
  in
  (* generate union type for shared context *)
  let gen_shared_union_type current_nsid name (spec : union_spec) =
    let type_name = get_shared_type_name current_nsid name in
    let is_closed = Option.value spec.closed ~default:false in
    emitln out (Printf.sprintf "type %s =" type_name) ;
    List.iter
      (fun ref_str ->
        let variant_name = Naming.qualified_variant_name_of_ref ref_str in
        let payload_type = gen_shared_ref_type current_nsid ref_str in
        emitln out (Printf.sprintf "  | %s of %s" variant_name payload_type) )
      spec.refs ;
    if not is_closed then emitln out "  | Unknown of Yojson.Safe.t" ;
    emit_newline out ;
    emitln out (Printf.sprintf "let %s_of_yojson json =" type_name) ;
    emitln out "  let open Yojson.Safe.Util in" ;
    emitln out "  try" ;
    emitln out "    match json |> member \"$type\" |> to_string with" ;
    List.iter
      (fun ref_str ->
        let variant_name = Naming.qualified_variant_name_of_ref ref_str in
        let full_type_uri = gen_shared_type_uri current_nsid ref_str in
        let payload_type = gen_shared_ref_type current_nsid ref_str in
        emitln out (Printf.sprintf "    | \"%s\" ->" full_type_uri) ;
        emitln out
          (Printf.sprintf "        (match %s_of_yojson json with" payload_type) ;
        emitln out (Printf.sprintf "         | Ok v -> Ok (%s v)" variant_name) ;
        emitln out "         | Error e -> Error e)" )
      spec.refs ;
    if is_closed then
      emitln out "    | t -> Error (\"unknown union type: \" ^ t)"
    else emitln out "    | _ -> Ok (Unknown json)" ;
    emitln out "  with _ -> Error \"failed to parse union\"" ;
    emit_newline out ;
    emitln out (Printf.sprintf "let %s_to_yojson = function" type_name) ;
    List.iter
      (fun ref_str ->
        let variant_name = Naming.qualified_variant_name_of_ref ref_str in
        let full_type_uri = gen_shared_type_uri current_nsid ref_str in
        let payload_type = gen_shared_ref_type current_nsid ref_str in
        emitln out (Printf.sprintf "  | %s v ->" variant_name) ;
        emitln out
          (Printf.sprintf "      (match %s_to_yojson v with" payload_type) ;
        emitln out
          (Printf.sprintf
             "       | `Assoc fields -> `Assoc ((\"$type\", `String \"%s\") :: \
              fields)"
             full_type_uri ) ;
        emitln out "       | other -> other)" )
      spec.refs ;
    if not is_closed then emitln out "  | Unknown j -> j" ;
    emit_newline out
  in
  (* collect refs for shared SCC detection, using compound keys (nsid#name) *)
  let collect_shared_local_refs current_nsid acc type_def =
    let rec aux acc = function
      | Array {items; _} ->
          aux acc items
      | Ref {ref_; _} ->
          if String.length ref_ > 0 && ref_.[0] = '#' then
            (* local ref: #foo -> current_nsid#foo *)
            let def_name = String.sub ref_ 1 (String.length ref_ - 1) in
            (current_nsid ^ "#" ^ def_name) :: acc
          else begin
            match String.split_on_char '#' ref_ with
            | [ext_nsid; def_name] when List.mem ext_nsid shared_nsids ->
                (* cross-nsid ref within shared group *)
                (ext_nsid ^ "#" ^ def_name) :: acc
            | _ ->
                acc
          end
      | Union {refs; _} ->
          List.fold_left
            (fun a r ->
              if String.length r > 0 && r.[0] = '#' then
                let def_name = String.sub r 1 (String.length r - 1) in
                (current_nsid ^ "#" ^ def_name) :: a
              else
                match String.split_on_char '#' r with
                | [ext_nsid; def_name] when List.mem ext_nsid shared_nsids ->
                    (ext_nsid ^ "#" ^ def_name) :: a
                | _ ->
                    a )
            acc refs
      | Object {properties; _} ->
          List.fold_left
            (fun a (_, (prop : property)) -> aux a prop.type_def)
            acc properties
      | Record {record; _} ->
          List.fold_left
            (fun a (_, (prop : property)) -> aux a prop.type_def)
            acc record.properties
      | Query {parameters; output; _} -> (
          let acc =
            match parameters with
            | Some params ->
                List.fold_left
                  (fun a (_, (prop : property)) -> aux a prop.type_def)
                  acc params.properties
            | None ->
                acc
          in
          match output with
          | Some body ->
              Option.fold ~none:acc ~some:(aux acc) body.schema
          | None ->
              acc )
      | Procedure {parameters; input; output; _} -> (
          let acc =
            match parameters with
            | Some params ->
                List.fold_left
                  (fun a (_, (prop : property)) -> aux a prop.type_def)
                  acc params.properties
            | None ->
                acc
          in
          let acc =
            match input with
            | Some body ->
                Option.fold ~none:acc ~some:(aux acc) body.schema
            | None ->
                acc
          in
          match output with
          | Some body ->
              Option.fold ~none:acc ~some:(aux acc) body.schema
          | None ->
              acc )
      | _ ->
          acc
    in
    aux acc type_def
  in
  (* generate single shared def *)
  let gen_shared_single_def (nsid, def) =
    match def.type_def with
    | Object spec ->
        gen_shared_object_type nsid def.name spec
    | Union spec ->
        gen_shared_union_type nsid def.name spec
    | Token spec ->
        gen_token nsid out def.name spec
    | Query spec ->
        gen_query nsid out def.name spec
    | Procedure spec ->
        gen_procedure nsid out def.name spec
    | Record spec ->
        gen_shared_object_type nsid def.name spec.record
    | String spec when spec.known_values <> None ->
        gen_string_type out def.name spec
    | Array {items; _} ->
        (* generate inline union for array items if needed *)
        ( match items with
        | Union spec ->
            let item_type_name = Naming.type_name (def.name ^ "_item") in
            register_union_name out spec.refs item_type_name ;
            gen_shared_union_type nsid (def.name ^ "_item") spec
        | _ ->
            () ) ;
        (* generate type alias for array *)
        let type_name = get_shared_type_name nsid def.name in
        let item_type = gen_shared_type_ref nsid items in
        emitln out (Printf.sprintf "type %s = %s list" type_name item_type) ;
        emitln out (Printf.sprintf "let %s_of_yojson json =" type_name) ;
        emitln out "  let open Yojson.Safe.Util in" ;
        emitln out
          (Printf.sprintf
             "  Ok (to_list json |> List.filter_map (fun x -> match \
              %s_of_yojson x with Ok v -> Some v | _ -> None))"
             item_type ) ;
        emitln out
          (Printf.sprintf "let %s_to_yojson l = `List (List.map %s_to_yojson l)"
             type_name item_type ) ;
        emit_newline out
    | _ ->
        ()
  in
  (* helper to generate object type definition only (no converters) *)
  let gen_shared_object_type_only ?(keyword = "type") nsid name
      (spec : object_spec) =
    let required = Option.value spec.required ~default:[] in
    let nullable = Option.value spec.nullable ~default:[] in
    let type_name = get_shared_type_name nsid name in
    if spec.properties = [] then
      emitln out (Printf.sprintf "%s %s = unit" keyword type_name)
    else begin
      emitln out (Printf.sprintf "%s %s = {" keyword type_name) ;
      List.iter
        (fun (prop_name, (prop : property)) ->
          let ocaml_name = Naming.field_name prop_name in
          let base_type = gen_shared_type_ref nsid prop.type_def in
          let is_required = List.mem prop_name required in
          let is_nullable = List.mem prop_name nullable in
          let type_str =
            if is_required && not is_nullable then base_type
            else base_type ^ " option"
          in
          let key_attr = Naming.key_annotation prop_name ocaml_name in
          let default_attr =
            if is_required && not is_nullable then "" else " [@default None]"
          in
          emitln out
            (Printf.sprintf "  %s: %s%s%s;" ocaml_name type_str key_attr
               default_attr ) )
        spec.properties ;
      emitln out "}"
    end
  in
  (* helper to generate inline union type definition only *)
  let gen_shared_inline_union_type_only ?(keyword = "type") nsid name refs spec
      =
    let is_closed = Option.value spec.closed ~default:false in
    emitln out (Printf.sprintf "%s %s =" keyword name) ;
    List.iter
      (fun ref_str ->
        let variant_name = Naming.qualified_variant_name_of_ref ref_str in
        let payload_type = gen_shared_ref_type nsid ref_str in
        emitln out (Printf.sprintf "  | %s of %s" variant_name payload_type) )
      refs ;
    if not is_closed then emitln out "  | Unknown of Yojson.Safe.t"
  in
  (* helper to generate object converters *)
  let gen_shared_object_converters ?(of_keyword = "let") ?(to_keyword = "let")
      nsid name (spec : object_spec) =
    let required = Option.value spec.required ~default:[] in
    let nullable = Option.value spec.nullable ~default:[] in
    let type_name = get_shared_type_name nsid name in
    if spec.properties = [] then begin
      if of_keyword <> "SKIP" then
        emitln out
          (Printf.sprintf "%s %s_of_yojson _ = Ok ()" of_keyword type_name) ;
      if to_keyword <> "SKIP" then
        emitln out
          (Printf.sprintf "%s %s_to_yojson () = `Assoc []" to_keyword type_name)
    end
    else begin
      (* of_yojson *)
      if of_keyword <> "SKIP" then begin
        emitln out
          (Printf.sprintf "%s %s_of_yojson json =" of_keyword type_name) ;
        emitln out "  let open Yojson.Safe.Util in" ;
        emitln out "  try" ;
        List.iter
          (fun (prop_name, (prop : property)) ->
            let ocaml_name = Naming.field_name prop_name in
            let conv_expr, needs_unwrap =
              gen_shared_of_yojson_expr nsid prop.type_def
            in
            let is_required = List.mem prop_name required in
            let is_nullable = List.mem prop_name nullable in
            let is_optional = (not is_required) || is_nullable in
            if is_optional then begin
              if needs_unwrap then
                emitln out
                  (Printf.sprintf
                     "    let %s = json |> member \"%s\" |> to_option (fun x \
                      -> match %s x with Ok v -> Some v | _ -> None) |> \
                      Option.join in"
                     ocaml_name prop_name conv_expr )
              else
                emitln out
                  (Printf.sprintf
                     "    let %s = json |> member \"%s\" |> to_option %s in"
                     ocaml_name prop_name conv_expr )
            end
            else begin
              if needs_unwrap then
                emitln out
                  (Printf.sprintf
                     "    let %s = json |> member \"%s\" |> %s |> \
                      Result.get_ok in"
                     ocaml_name prop_name conv_expr )
              else
                emitln out
                  (Printf.sprintf "    let %s = json |> member \"%s\" |> %s in"
                     ocaml_name prop_name conv_expr )
            end )
          spec.properties ;
        emit out "    Ok { " ;
        emit out
          (String.concat "; "
             (List.map (fun (pn, _) -> Naming.field_name pn) spec.properties) ) ;
        emitln out " }" ;
        emitln out "  with e -> Error (Printexc.to_string e)" ;
        emit_newline out
      end ;
      (* to_yojson *)
      if to_keyword <> "SKIP" then begin
        emitln out
          (Printf.sprintf "%s %s_to_yojson (r : %s) =" to_keyword type_name
             type_name ) ;
        emitln out "  `Assoc [" ;
        List.iteri
          (fun i (prop_name, (prop : property)) ->
            let ocaml_name = Naming.field_name prop_name in
            let conv_expr = gen_shared_to_yojson_expr nsid prop.type_def in
            let is_required = List.mem prop_name required in
            let is_nullable = List.mem prop_name nullable in
            let is_optional = (not is_required) || is_nullable in
            let comma =
              if i < List.length spec.properties - 1 then ";" else ""
            in
            if is_optional then
              emitln out
                (Printf.sprintf
                   "    (\"%s\", match r.%s with Some v -> %s v | None -> \
                    `Null)%s"
                   prop_name ocaml_name conv_expr comma )
            else
              emitln out
                (Printf.sprintf "    (\"%s\", %s r.%s)%s" prop_name conv_expr
                   ocaml_name comma ) )
          spec.properties ;
        emitln out "  ]" ;
        emit_newline out
      end
    end
  in
  (* helper to generate inline union converters *)
  let gen_shared_inline_union_converters ?(of_keyword = "let")
      ?(to_keyword = "let") nsid name refs spec =
    let is_closed = Option.value spec.closed ~default:false in
    (* of_yojson *)
    if of_keyword <> "SKIP" then begin
      emitln out (Printf.sprintf "%s %s_of_yojson json =" of_keyword name) ;
      emitln out "  let open Yojson.Safe.Util in" ;
      emitln out "  try" ;
      emitln out "    match json |> member \"$type\" |> to_string with" ;
      List.iter
        (fun ref_str ->
          let variant_name = Naming.qualified_variant_name_of_ref ref_str in
          let full_type_uri = gen_shared_type_uri nsid ref_str in
          let payload_type = gen_shared_ref_type nsid ref_str in
          emitln out (Printf.sprintf "    | \"%s\" ->" full_type_uri) ;
          emitln out
            (Printf.sprintf "        (match %s_of_yojson json with" payload_type) ;
          emitln out
            (Printf.sprintf "         | Ok v -> Ok (%s v)" variant_name) ;
          emitln out "         | Error e -> Error e)" )
        refs ;
      if is_closed then
        emitln out "    | t -> Error (\"unknown union type: \" ^ t)"
      else emitln out "    | _ -> Ok (Unknown json)" ;
      emitln out "  with _ -> Error \"failed to parse union\"" ;
      emit_newline out
    end ;
    (* to_yojson *)
    if to_keyword <> "SKIP" then begin
      emitln out (Printf.sprintf "%s %s_to_yojson = function" to_keyword name) ;
      List.iter
        (fun ref_str ->
          let variant_name = Naming.qualified_variant_name_of_ref ref_str in
          let full_type_uri = gen_shared_type_uri nsid ref_str in
          let payload_type = gen_shared_ref_type nsid ref_str in
          emitln out (Printf.sprintf "  | %s v ->" variant_name) ;
          emitln out
            (Printf.sprintf "      (match %s_to_yojson v with" payload_type) ;
          emitln out
            (Printf.sprintf
               "       | `Assoc fields -> `Assoc ((\"$type\", `String \"%s\") \
                :: fields)"
               full_type_uri ) ;
          emitln out "       | other -> other)" )
        refs ;
      if not is_closed then emitln out "  | Unknown j -> j" ;
      emit_newline out
    end
  in
  (* collect all inline unions as pseudo-defs for proper ordering *)
  let rec collect_inline_unions_from_type nsid context acc type_def =
    match type_def with
    | Union spec ->
        let union_name = get_shared_inline_union_name nsid context in
        (nsid, union_name, spec.refs, spec) :: acc
    | Array {items; _} ->
        collect_inline_unions_from_type nsid (context ^ "_item") acc items
    | Object {properties; _} ->
        List.fold_left
          (fun a (prop_name, (prop : property)) ->
            collect_inline_unions_from_type nsid prop_name a prop.type_def )
          acc properties
    | _ ->
        acc
  in
  let all_inline_unions =
    List.concat_map
      (fun (nsid, def) ->
        match def.type_def with
        | Object spec ->
            List.fold_left
              (fun acc (prop_name, (prop : property)) ->
                collect_inline_unions_from_type nsid prop_name acc prop.type_def )
              [] spec.properties
        | Record spec ->
            List.fold_left
              (fun acc (prop_name, (prop : property)) ->
                collect_inline_unions_from_type nsid prop_name acc prop.type_def )
              [] spec.record.properties
        | _ ->
            [] )
      all_defs
  in
  (* create inline union entries *)
  let inline_union_defs =
    List.map
      (fun (nsid, name, refs, spec) ->
        let key = nsid ^ "#__inline__" ^ name in
        let deps =
          List.filter_map
            (fun r ->
              if String.length r > 0 && r.[0] = '#' then
                let def_name = String.sub r 1 (String.length r - 1) in
                Some (nsid ^ "#" ^ def_name)
              else
                match String.split_on_char '#' r with
                | [ext_nsid; def_name] when List.mem ext_nsid shared_nsids ->
                    Some (ext_nsid ^ "#" ^ def_name)
                | _ ->
                    None )
            refs
        in
        (key, deps, `InlineUnion (nsid, name, refs, spec)) )
      all_inline_unions
  in
  (* create regular def entries *)
  let regular_def_entries =
    List.map
      (fun (nsid, def) ->
        let key = nsid ^ "#" ^ def.name in
        let base_deps = collect_shared_local_refs nsid [] def.type_def in
        let inline_deps =
          match def.type_def with
          | Object spec | Record {record= spec; _} ->
              List.fold_left
                (fun acc (prop_name, (prop : property)) ->
                  match prop.type_def with
                  | Union _ ->
                      let union_name =
                        get_shared_inline_union_name nsid prop_name
                      in
                      (nsid ^ "#__inline__" ^ union_name) :: acc
                  | Array {items= Union _; _} ->
                      let union_name =
                        get_shared_inline_union_name nsid (prop_name ^ "_item")
                      in
                      (nsid ^ "#__inline__" ^ union_name) :: acc
                  | _ ->
                      acc )
                [] spec.properties
          | _ ->
              []
        in
        (key, base_deps @ inline_deps, `RegularDef (nsid, def)) )
      all_defs
  in
  (* combine all entries *)
  let all_entries = regular_def_entries @ inline_union_defs in
  let deps_map = List.map (fun (k, deps, _) -> (k, deps)) all_entries in
  let entry_map = List.map (fun (k, _, entry) -> (k, entry)) all_entries in
  let all_keys = List.map (fun (k, _, _) -> k) all_entries in
  (* run Tarjan's algorithm *)
  let index_counter = ref 0 in
  let indices = Hashtbl.create 64 in
  let lowlinks = Hashtbl.create 64 in
  let on_stack = Hashtbl.create 64 in
  let stack = ref [] in
  let sccs = ref [] in
  let rec strongconnect key =
    let index = !index_counter in
    incr index_counter ;
    Hashtbl.add indices key index ;
    Hashtbl.add lowlinks key index ;
    Hashtbl.add on_stack key true ;
    stack := key :: !stack ;
    let successors =
      try List.assoc key deps_map |> List.filter (fun k -> List.mem k all_keys)
      with Not_found -> []
    in
    List.iter
      (fun succ ->
        if not (Hashtbl.mem indices succ) then begin
          strongconnect succ ;
          Hashtbl.replace lowlinks key
            (min (Hashtbl.find lowlinks key) (Hashtbl.find lowlinks succ))
        end
        else if Hashtbl.find_opt on_stack succ = Some true then
          Hashtbl.replace lowlinks key
            (min (Hashtbl.find lowlinks key) (Hashtbl.find indices succ)) )
      successors ;
    if Hashtbl.find lowlinks key = Hashtbl.find indices key then begin
      let rec pop_scc acc =
        match !stack with
        | [] ->
            acc
        | top :: rest ->
            stack := rest ;
            Hashtbl.replace on_stack top false ;
            if top = key then top :: acc else pop_scc (top :: acc)
      in
      let scc_keys = pop_scc [] in
      let scc_entries =
        List.filter_map (fun k -> List.assoc_opt k entry_map) scc_keys
      in
      if scc_entries <> [] then sccs := scc_entries :: !sccs
    end
  in
  List.iter
    (fun key -> if not (Hashtbl.mem indices key) then strongconnect key)
    all_keys ;
  let ordered_sccs = List.rev !sccs in
  (* generate each SCC *)
  List.iter
    (fun scc ->
      let inline_unions_in_scc =
        List.filter_map (function `InlineUnion x -> Some x | _ -> None) scc
      in
      let regular_defs_in_scc =
        List.filter_map (function `RegularDef x -> Some x | _ -> None) scc
      in
      if inline_unions_in_scc = [] then begin
        (* no inline unions - check if we still need mutual recursion *)
        match regular_defs_in_scc with
        | [] ->
            ()
        | [(nsid, def)] ->
            (* single def, generate normally *)
            gen_shared_single_def (nsid, def)
        | defs ->
            (* multiple defs in SCC - need mutual recursion *)
            (* filter to only object-like types that can be mutually recursive *)
            let obj_defs =
              List.filter
                (fun (_, def) ->
                  match def.type_def with
                  | Object _ | Record _ ->
                      true
                  | _ ->
                      false )
                defs
            in
            let other_defs =
              List.filter
                (fun (_, def) ->
                  match def.type_def with
                  | Object _ | Record _ ->
                      false
                  | _ ->
                      true )
                defs
            in
            (* generate non-object types first (they have their own converters) *)
            List.iter gen_shared_single_def other_defs ;
            (* generate object types as mutually recursive *)
            if obj_defs <> [] then begin
              (* register inline unions from all objects first *)
              List.iter
                (fun (nsid, def) ->
                  match def.type_def with
                  | Object spec ->
                      register_shared_inline_unions nsid spec.properties
                  | Record rspec ->
                      register_shared_inline_unions nsid rspec.record.properties
                  | _ ->
                      () )
                obj_defs ;
              (* generate all type definitions *)
              List.iteri
                (fun i (nsid, def) ->
                  let keyword = if i = 0 then "type" else "and" in
                  match def.type_def with
                  | Object spec ->
                      gen_shared_object_type_only ~keyword nsid def.name spec
                  | Record rspec ->
                      gen_shared_object_type_only ~keyword nsid def.name
                        rspec.record
                  | _ ->
                      () )
                obj_defs ;
              emit_newline out ;
              (* generate all _of_yojson converters as mutually recursive *)
              List.iteri
                (fun i (nsid, def) ->
                  let of_keyword = if i = 0 then "let rec" else "and" in
                  match def.type_def with
                  | Object spec ->
                      gen_shared_object_converters ~of_keyword
                        ~to_keyword:"SKIP" nsid def.name spec
                  | Record rspec ->
                      gen_shared_object_converters ~of_keyword
                        ~to_keyword:"SKIP" nsid def.name rspec.record
                  | _ ->
                      () )
                obj_defs ;
              (* generate all _to_yojson converters *)
              List.iter
                (fun (nsid, def) ->
                  match def.type_def with
                  | Object spec ->
                      gen_shared_object_converters ~of_keyword:"SKIP"
                        ~to_keyword:"and" nsid def.name spec
                  | Record rspec ->
                      gen_shared_object_converters ~of_keyword:"SKIP"
                        ~to_keyword:"and" nsid def.name rspec.record
                  | _ ->
                      () )
                obj_defs
            end
      end
      else begin
        (* has inline unions - generate all types first, then all converters *)
        List.iter
          (fun (_nsid, name, refs, _spec) ->
            register_union_name out refs name ;
            mark_union_generated out name )
          inline_unions_in_scc ;
        let all_items =
          List.map (fun x -> `Inline x) inline_unions_in_scc
          @ List.map (fun x -> `Regular x) regular_defs_in_scc
        in
        let n = List.length all_items in
        if n = 1 then begin
          match List.hd all_items with
          | `Inline (nsid, name, refs, spec) ->
              gen_shared_inline_union_type_only nsid name refs spec ;
              emit_newline out ;
              gen_shared_inline_union_converters nsid name refs spec
          | `Regular (nsid, def) -> (
            match def.type_def with
            | Object spec ->
                register_shared_inline_unions nsid spec.properties ;
                gen_shared_object_type_only nsid def.name spec ;
                emit_newline out ;
                gen_shared_object_converters nsid def.name spec
            | Record rspec ->
                register_shared_inline_unions nsid rspec.record.properties ;
                gen_shared_object_type_only nsid def.name rspec.record ;
                emit_newline out ;
                gen_shared_object_converters nsid def.name rspec.record
            | _ ->
                gen_shared_single_def (nsid, def) )
        end
        else begin
          (* multiple items - generate as mutually recursive types *)
          List.iter
            (function
              | `Regular (nsid, def) -> (
                match def.type_def with
                | Object spec ->
                    register_shared_inline_unions nsid spec.properties
                | Record rspec ->
                    register_shared_inline_unions nsid rspec.record.properties
                | _ ->
                    () )
              | `Inline _ ->
                  () )
            all_items ;
          (* generate all type definitions *)
          List.iteri
            (fun i item ->
              let keyword = if i = 0 then "type" else "and" in
              match item with
              | `Inline (nsid, name, refs, spec) ->
                  gen_shared_inline_union_type_only ~keyword nsid name refs spec
              | `Regular (nsid, def) -> (
                match def.type_def with
                | Object spec ->
                    gen_shared_object_type_only ~keyword nsid def.name spec
                | Record rspec ->
                    gen_shared_object_type_only ~keyword nsid def.name
                      rspec.record
                | _ ->
                    () ) )
            all_items ;
          emit_newline out ;
          (* generate all _of_yojson converters *)
          List.iteri
            (fun i item ->
              let of_keyword = if i = 0 then "let rec" else "and" in
              match item with
              | `Inline (nsid, name, refs, spec) ->
                  gen_shared_inline_union_converters ~of_keyword
                    ~to_keyword:"SKIP" nsid name refs spec
              | `Regular (nsid, def) -> (
                match def.type_def with
                | Object spec ->
                    gen_shared_object_converters ~of_keyword ~to_keyword:"SKIP"
                      nsid def.name spec
                | Record rspec ->
                    gen_shared_object_converters ~of_keyword ~to_keyword:"SKIP"
                      nsid def.name rspec.record
                | _ ->
                    () ) )
            all_items ;
          (* generate all _to_yojson converters *)
          List.iteri
            (fun i item ->
              let to_keyword = "and" in
              ignore i ;
              match item with
              | `Inline (nsid, name, refs, spec) ->
                  gen_shared_inline_union_converters ~of_keyword:"SKIP"
                    ~to_keyword nsid name refs spec
              | `Regular (nsid, def) -> (
                match def.type_def with
                | Object spec ->
                    gen_shared_object_converters ~of_keyword:"SKIP" ~to_keyword
                      nsid def.name spec
                | Record rspec ->
                    gen_shared_object_converters ~of_keyword:"SKIP" ~to_keyword
                      nsid def.name rspec.record
                | _ ->
                    () ) )
            all_items
        end
      end )
    ordered_sccs ;
  Emitter.contents out

(* generate a re-export module that maps local names to shared module types *)
let gen_reexport_module ~shared_module_name ~all_merged_docs (doc : lexicon_doc)
    : string =
  let buf = Buffer.create 1024 in
  let emit s = Buffer.add_string buf s in
  let emitln s = Buffer.add_string buf s ; Buffer.add_char buf '\n' in
  (* detect collisions across all merged docs *)
  let all_defs =
    List.concat_map
      (fun d -> List.map (fun def -> (d.id, def)) d.defs)
      all_merged_docs
  in
  let name_counts = Hashtbl.create 64 in
  List.iter
    (fun (nsid, def) ->
      let existing = Hashtbl.find_opt name_counts def.name in
      match existing with
      | None ->
          Hashtbl.add name_counts def.name [nsid]
      | Some nsids when not (List.mem nsid nsids) ->
          Hashtbl.replace name_counts def.name (nsid :: nsids)
      | _ ->
          () )
    all_defs ;
  let colliding_names =
    Hashtbl.fold
      (fun name nsids acc -> if List.length nsids > 1 then name :: acc else acc)
      name_counts []
  in
  (* function to get shared type name (context-based for collisions) *)
  let get_shared_type_name nsid def_name =
    if List.mem def_name colliding_names then
      Naming.shared_type_name nsid def_name
    else Naming.type_name def_name
  in
  emitln (Printf.sprintf "(* re-exported from %s *)" shared_module_name) ;
  emitln "" ;
  List.iter
    (fun def ->
      let local_type_name = Naming.type_name def.name in
      let shared_type_name = get_shared_type_name doc.id def.name in
      match def.type_def with
      | Object _ | Record _ | Union _ ->
          emitln
            (Printf.sprintf "type %s = %s.%s" local_type_name shared_module_name
               shared_type_name ) ;
          emitln
            (Printf.sprintf "let %s_of_yojson = %s.%s_of_yojson" local_type_name
               shared_module_name shared_type_name ) ;
          emitln
            (Printf.sprintf "let %s_to_yojson = %s.%s_to_yojson" local_type_name
               shared_module_name shared_type_name ) ;
          emit "\n"
      | String spec when spec.known_values <> None ->
          emitln
            (Printf.sprintf "type %s = %s.%s" local_type_name shared_module_name
               shared_type_name ) ;
          emitln
            (Printf.sprintf "let %s_of_yojson = %s.%s_of_yojson" local_type_name
               shared_module_name shared_type_name ) ;
          emitln
            (Printf.sprintf "let %s_to_yojson = %s.%s_to_yojson" local_type_name
               shared_module_name shared_type_name ) ;
          emit "\n"
      | Array _ ->
          emitln
            (Printf.sprintf "type %s = %s.%s" local_type_name shared_module_name
               shared_type_name ) ;
          emitln
            (Printf.sprintf "let %s_of_yojson = %s.%s_of_yojson" local_type_name
               shared_module_name shared_type_name ) ;
          emitln
            (Printf.sprintf "let %s_to_yojson = %s.%s_to_yojson" local_type_name
               shared_module_name shared_type_name ) ;
          emit "\n"
      | Token _ ->
          emitln
            (Printf.sprintf "let %s = %s.%s" local_type_name shared_module_name
               shared_type_name ) ;
          emit "\n"
      | Query _ | Procedure _ ->
          let mod_name = Naming.def_module_name def.name in
          emitln
            (Printf.sprintf "module %s = %s.%s" mod_name shared_module_name
               mod_name ) ;
          emit "\n"
      | _ ->
          () )
    doc.defs ;
  Buffer.contents buf
