open Lexicon_types

type output =
  { mutable imports: string list
  ; mutable generated_unions: string list
  ; buf: Buffer.t }

let make_output () = {imports= []; generated_unions= []; buf= Buffer.create 4096}

let add_import out module_name =
  if not (List.mem module_name out.imports) then
    out.imports <- module_name :: out.imports

let mark_union_generated out union_name =
  if not (List.mem union_name out.generated_unions) then
    out.generated_unions <- union_name :: out.generated_unions

let is_union_generated out union_name = List.mem union_name out.generated_unions

let emit out s = Buffer.add_string out.buf s

let emitln out s =
  Buffer.add_string out.buf s ;
  Buffer.add_char out.buf '\n'

let emit_newline out = Buffer.add_char out.buf '\n'

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
  | Union {refs; _} ->
      (* generate inline union reference *)
      gen_union_type_name refs
  | Token _ ->
      "string"
  | Unknown _ ->
      "Yojson.Safe.t"
  | Query _ | Procedure _ | Subscription _ | Record _ ->
      "unit (* primary type *)"

(* generate reference to another type *)
and gen_ref_type _nsid out ref_str : string =
  if String.length ref_str > 0 && ref_str.[0] = '#' then begin
    (* local ref: #someDef -> someDef *)
    let def_name = String.sub ref_str 1 (String.length ref_str - 1) in
    Naming.type_name def_name
  end
  else begin
    (* external ref: com.example.defs#someDef *)
    match String.split_on_char '#' ref_str with
    | [ext_nsid; def_name] ->
        (* use flat module names for include_subdirs unqualified *)
        let flat_module = Naming.flat_module_name_of_nsid ext_nsid in
        add_import out flat_module ;
        flat_module ^ "." ^ Naming.type_name def_name
    | [ext_nsid] ->
        (* just nsid, refers to main def *)
        let flat_module = Naming.flat_module_name_of_nsid ext_nsid in
        add_import out flat_module ; flat_module ^ ".main"
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

(* collect inline union specs from object properties *)
let rec collect_inline_unions acc type_def =
  match type_def with
  | Union spec ->
      (spec.refs, spec) :: acc
  | Array {items; _} ->
      collect_inline_unions acc items
  | _ ->
      acc

let collect_inline_unions_from_properties properties =
  List.fold_left
    (fun acc (_, (prop : property)) -> collect_inline_unions acc prop.type_def)
    [] properties

(* generate inline union types that appear in object properties *)
let gen_inline_unions nsid out properties =
  let inline_unions = collect_inline_unions_from_properties properties in
  List.iter
    (fun (refs, spec) ->
      let type_name = Naming.union_type_name refs in
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
            let payload_type = gen_ref_type nsid out ref_str in
            emitln out
              (Printf.sprintf "  | %s v -> %s_to_yojson v" variant_name
                 payload_type ) )
          refs ;
        if not is_closed then emitln out "  | Unknown j -> j" ;
        emit_newline out
      end )
    inline_unions

(* generate object type definition *)
let gen_object_type nsid out name (spec : object_spec) =
  let required = Option.value spec.required ~default:[] in
  let nullable = Option.value spec.nullable ~default:[] in
  (* handle empty objects as unit *)
  if spec.properties = [] then begin
    emitln out (Printf.sprintf "type %s = unit" (Naming.type_name name)) ;
    emitln out
      (Printf.sprintf "let %s_of_yojson _ = Ok ()" (Naming.type_name name)) ;
    emitln out
      (Printf.sprintf "let %s_to_yojson () = `Assoc []" (Naming.type_name name)) ;
    emit_newline out
  end
  else begin
    (* generate inline union types first *)
    gen_inline_unions nsid out spec.properties ;
    emitln out (Printf.sprintf "type %s =" (Naming.type_name name)) ;
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
  (* generate to_yojson function *)
  emitln out (Printf.sprintf "let %s_to_yojson = function" type_name) ;
  List.iter
    (fun ref_str ->
      let variant_name = Naming.variant_name_of_ref ref_str in
      let payload_type = gen_ref_type nsid out ref_str in
      emitln out
        (Printf.sprintf "  | %s v -> %s_to_yojson v" variant_name payload_type) )
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
      emitln out "  (** Raw bytes output with content type *)" ;
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
      emitln out "  (** Raw bytes output with content type *)" ;
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
          emit out "    let input = Some ({" ;
          let fields =
            List.map
              (fun (prop_name, _) -> Naming.field_name prop_name)
              obj_spec.properties
          in
          emit out (String.concat "; " fields) ;
          emitln out "} |> input_to_yojson) in"
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
let gen_string_type _nsid out name (spec : string_spec) =
  let type_name = Naming.type_name name in
  emitln out
    (Printf.sprintf "(** String type with known values%s *)"
       (match spec.description with Some d -> ": " ^ d | None -> "") ) ;
  emitln out (Printf.sprintf "type %s = string" type_name) ;
  emitln out (Printf.sprintf "let %s_of_yojson = function" type_name) ;
  emitln out "  | `String s -> Ok s" ;
  emitln out (Printf.sprintf "  | _ -> Error \"%s: expected string\"" type_name) ;
  emitln out (Printf.sprintf "let %s_to_yojson s = `String s" type_name) ;
  emit_newline out

(* collect local refs from a type definition *)
let rec collect_local_refs acc = function
  | Array {items; _} ->
      collect_local_refs acc items
  | Ref {ref_; _} ->
      if String.length ref_ > 0 && ref_.[0] = '#' then
        let def_name = String.sub ref_ 1 (String.length ref_ - 1) in
        def_name :: acc
      else acc
  | Union {refs; _} ->
      List.fold_left
        (fun a r ->
          if String.length r > 0 && r.[0] = '#' then
            let def_name = String.sub r 1 (String.length r - 1) in
            def_name :: a
          else a )
        acc refs
  | Object {properties; _} ->
      List.fold_left
        (fun a (_, (prop : property)) -> collect_local_refs a prop.type_def)
        acc properties
  | Record {record; _} ->
      List.fold_left
        (fun a (_, (prop : property)) -> collect_local_refs a prop.type_def)
        acc record.properties
  | Query {parameters; output; _} -> (
      let acc =
        match parameters with
        | Some params ->
            List.fold_left
              (fun a (_, (prop : property)) ->
                collect_local_refs a prop.type_def )
              acc params.properties
        | None ->
            acc
      in
      match output with
      | Some body ->
          Option.fold ~none:acc ~some:(collect_local_refs acc) body.schema
      | None ->
          acc )
  | Procedure {parameters; input; output; _} -> (
      let acc =
        match parameters with
        | Some params ->
            List.fold_left
              (fun a (_, (prop : property)) ->
                collect_local_refs a prop.type_def )
              acc params.properties
        | None ->
            acc
      in
      let acc =
        match input with
        | Some body ->
            Option.fold ~none:acc ~some:(collect_local_refs acc) body.schema
        | None ->
            acc
      in
      match output with
      | Some body ->
          Option.fold ~none:acc ~some:(collect_local_refs acc) body.schema
      | None ->
          acc )
  | _ ->
      acc

(* sort definitions so dependencies come first *)
let sort_definitions (defs : def_entry list) : def_entry list =
  (* build dependency map: name -> list of dependencies *)
  let deps =
    List.map (fun def -> (def.name, collect_local_refs [] def.type_def)) defs
  in
  (* create name -> def map *)
  let def_map = List.fold_left (fun m def -> (def.name, def) :: m) [] defs in
  (* topological sort *)
  let rec visit visited sorted name =
    if List.mem name visited then (visited, sorted)
    else
      let visited = name :: visited in
      let dep_names = try List.assoc name deps with Not_found -> [] in
      let visited, sorted =
        List.fold_left (fun (v, s) d -> visit v s d) (visited, sorted) dep_names
      in
      let sorted =
        match List.assoc_opt name def_map with
        | Some def ->
            def :: sorted
        | None ->
            sorted
      in
      (visited, sorted)
  in
  let _, sorted =
    List.fold_left (fun (v, s) def -> visit v s def.name) ([], []) defs
  in
  (* sorted is in reverse order, reverse it *)
  List.rev sorted

(* generate complete lexicon module *)
let gen_lexicon_module (doc : lexicon_doc) : string =
  let out = make_output () in
  let nsid = doc.id in
  (* header *)
  emitln out (Printf.sprintf "(* generated from %s *)" nsid) ;
  emit_newline out ;
  (* sort definitions by dependencies *)
  let sorted_defs = sort_definitions doc.defs in
  (* generate each definition *)
  List.iter
    (fun def ->
      match def.type_def with
      | Object spec ->
          gen_object_type nsid out def.name spec
      | Union spec ->
          gen_union_type nsid out def.name spec
      | Token spec ->
          gen_token nsid out def.name spec
      | Query spec ->
          gen_query nsid out def.name spec
      | Procedure spec ->
          gen_procedure nsid out def.name spec
      | Record spec ->
          (* generate record as object type *)
          gen_object_type nsid out def.name spec.record
      | String spec when spec.known_values <> None ->
          (* generate type alias for strings with known values *)
          gen_string_type nsid out def.name spec
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
          (* these are typically not standalone definitions *)
          () )
    sorted_defs ;
  Buffer.contents out.buf

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
  out.imports
