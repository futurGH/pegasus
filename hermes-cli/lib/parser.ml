(* parse lexicon json files into lexicon_types *)

open Lexicon_types

let get_string_opt key json =
  match json with
  | `Assoc pairs -> (
    match List.assoc_opt key pairs with Some (`String s) -> Some s | _ -> None )
  | _ ->
      None

let get_string key json =
  match get_string_opt key json with
  | Some s ->
      s
  | None ->
      failwith ("missing required string field: " ^ key)

let get_int_opt key json =
  match json with
  | `Assoc pairs -> (
    match List.assoc_opt key pairs with Some (`Int i) -> Some i | _ -> None )
  | _ ->
      None

let get_int key json =
  match get_int_opt key json with
  | Some i ->
      i
  | None ->
      failwith ("missing required int field: " ^ key)

let get_bool_opt key json =
  match json with
  | `Assoc pairs -> (
    match List.assoc_opt key pairs with Some (`Bool b) -> Some b | _ -> None )
  | _ ->
      None

let get_list_opt key json =
  match json with
  | `Assoc pairs -> (
    match List.assoc_opt key pairs with Some (`List l) -> Some l | _ -> None )
  | _ ->
      None

let get_string_list_opt key json =
  match get_list_opt key json with
  | Some l ->
      Some (List.filter_map (function `String s -> Some s | _ -> None) l)
  | None ->
      None

let get_int_list_opt key json =
  match get_list_opt key json with
  | Some l ->
      Some (List.filter_map (function `Int i -> Some i | _ -> None) l)
  | None ->
      None

let get_assoc key json =
  match json with
  | `Assoc pairs -> (
    match List.assoc_opt key pairs with
    | Some (`Assoc _ as a) ->
        Some a
    | _ ->
        None )
  | _ ->
      None

(* parse type definition from json *)
let rec parse_type_def json : type_def =
  let type_str = get_string "type" json in
  match type_str with
  | "string" ->
      String
        { format= get_string_opt "format" json
        ; min_length= get_int_opt "minLength" json
        ; max_length= get_int_opt "maxLength" json
        ; min_graphemes= get_int_opt "minGraphemes" json
        ; max_graphemes= get_int_opt "maxGraphemes" json
        ; known_values= get_string_list_opt "knownValues" json
        ; enum= get_string_list_opt "enum" json
        ; const= get_string_opt "const" json
        ; default= get_string_opt "default" json
        ; description= get_string_opt "description" json }
  | "integer" ->
      Integer
        { minimum= get_int_opt "minimum" json
        ; maximum= get_int_opt "maximum" json
        ; enum= get_int_list_opt "enum" json
        ; const= get_int_opt "const" json
        ; default= get_int_opt "default" json
        ; description= get_string_opt "description" json }
  | "boolean" ->
      Boolean
        { const= get_bool_opt "const" json
        ; default= get_bool_opt "default" json
        ; description= get_string_opt "description" json }
  | "bytes" ->
      Bytes
        { min_length= get_int_opt "minLength" json
        ; max_length= get_int_opt "maxLength" json
        ; description= get_string_opt "description" json }
  | "blob" ->
      Blob
        { accept= get_string_list_opt "accept" json
        ; max_size= get_int_opt "maxSize" json
        ; description= get_string_opt "description" json }
  | "cid-link" ->
      CidLink {description= get_string_opt "description" json}
  | "array" ->
      let items_json =
        match get_assoc "items" json with
        | Some j ->
            j
        | None ->
            failwith "array type missing items"
      in
      Array
        { items= parse_type_def items_json
        ; min_length= get_int_opt "minLength" json
        ; max_length= get_int_opt "maxLength" json
        ; description= get_string_opt "description" json }
  | "object" ->
      Object (parse_object_spec json)
  | "ref" ->
      Ref
        { ref_= get_string "ref" json
        ; description= get_string_opt "description" json }
  | "union" ->
      Union
        { refs=
            ( match get_string_list_opt "refs" json with
            | Some l ->
                l
            | None ->
                [] )
        ; closed= get_bool_opt "closed" json
        ; description= get_string_opt "description" json }
  | "token" ->
      Token {description= get_string_opt "description" json}
  | "unknown" ->
      Unknown {description= get_string_opt "description" json}
  | "query" ->
      Query (parse_query_spec json)
  | "procedure" ->
      Procedure (parse_procedure_spec json)
  | "subscription" ->
      Subscription (parse_subscription_spec json)
  | "record" ->
      Record (parse_record_spec json)
  | t ->
      failwith ("unknown type: " ^ t)

and parse_object_spec json : object_spec =
  let properties =
    match get_assoc "properties" json with
    | Some (`Assoc pairs) ->
        List.map
          (fun (name, prop_json) ->
            let type_def = parse_type_def prop_json in
            let description = get_string_opt "description" prop_json in
            (name, {type_def; description}) )
          pairs
    | _ ->
        []
  in
  { properties
  ; required= get_string_list_opt "required" json
  ; nullable= get_string_list_opt "nullable" json
  ; description= get_string_opt "description" json }

and parse_params_spec json : params_spec =
  let properties =
    match get_assoc "properties" json with
    | Some (`Assoc pairs) ->
        List.map
          (fun (name, prop_json) ->
            let type_def = parse_type_def prop_json in
            let description = get_string_opt "description" prop_json in
            (name, {type_def; description}) )
          pairs
    | _ ->
        []
  in
  { properties
  ; required= get_string_list_opt "required" json
  ; description= get_string_opt "description" json }

and parse_body_def json : body_def =
  { encoding= get_string "encoding" json
  ; schema=
      ( match get_assoc "schema" json with
      | Some j ->
          Some (parse_type_def j)
      | None ->
          None )
  ; description= get_string_opt "description" json }

and parse_error_def json : error_def =
  {name= get_string "name" json; description= get_string_opt "description" json}

and parse_query_spec json : query_spec =
  let parameters =
    match get_assoc "parameters" json with
    | Some j ->
        Some (parse_params_spec j)
    | None ->
        None
  in
  let output =
    match get_assoc "output" json with
    | Some j ->
        Some (parse_body_def j)
    | None ->
        None
  in
  let errors =
    match get_list_opt "errors" json with
    | Some l ->
        Some
          (List.map
             (function
               | `Assoc _ as j ->
                   parse_error_def j
               | _ ->
                   failwith "invalid error def" )
             l )
    | None ->
        None
  in
  {parameters; output; errors; description= get_string_opt "description" json}

and parse_procedure_spec json : procedure_spec =
  let parameters =
    match get_assoc "parameters" json with
    | Some j ->
        Some (parse_params_spec j)
    | None ->
        None
  in
  let input =
    match get_assoc "input" json with
    | Some j ->
        Some (parse_body_def j)
    | None ->
        None
  in
  let output =
    match get_assoc "output" json with
    | Some j ->
        Some (parse_body_def j)
    | None ->
        None
  in
  let errors =
    match get_list_opt "errors" json with
    | Some l ->
        Some
          (List.map
             (function
               | `Assoc _ as j ->
                   parse_error_def j
               | _ ->
                   failwith "invalid error def" )
             l )
    | None ->
        None
  in
  { parameters
  ; input
  ; output
  ; errors
  ; description= get_string_opt "description" json }

and parse_subscription_spec json : subscription_spec =
  let parameters =
    match get_assoc "parameters" json with
    | Some j ->
        Some (parse_params_spec j)
    | None ->
        None
  in
  let message =
    match get_assoc "message" json with
    | Some j ->
        Some (parse_body_def j)
    | None ->
        None
  in
  let errors =
    match get_list_opt "errors" json with
    | Some l ->
        Some
          (List.map
             (function
               | `Assoc _ as j ->
                   parse_error_def j
               | _ ->
                   failwith "invalid error def" )
             l )
    | None ->
        None
  in
  {parameters; message; errors; description= get_string_opt "description" json}

and parse_record_spec json : record_spec =
  let key = get_string "key" json in
  let record_json =
    match get_assoc "record" json with
    | Some j ->
        j
    | None ->
        failwith "record type missing record field"
  in
  { key
  ; record= parse_object_spec record_json
  ; description= get_string_opt "description" json }

(* parse complete lexicon document *)
let parse_lexicon_doc json : lexicon_doc =
  let lexicon = get_int "lexicon" json in
  let id = get_string "id" json in
  let revision = get_int_opt "revision" json in
  let description = get_string_opt "description" json in
  let defs =
    match get_assoc "defs" json with
    | Some (`Assoc pairs) ->
        List.map
          (fun (name, def_json) -> {name; type_def= parse_type_def def_json})
          pairs
    | _ ->
        []
  in
  {lexicon; id; revision; description; defs}

(* parse lexicon file *)
let parse_file path : parse_result =
  try
    let json = Yojson.Safe.from_file path in
    Ok (parse_lexicon_doc json)
  with
  | Yojson.Json_error e ->
      Error ("JSON parse error: " ^ e)
  | Failure e ->
      Error ("Parse error: " ^ e)
  | e ->
      Error ("Unexpected error: " ^ Printexc.to_string e)

(* parse json string *)
let parse_string content : parse_result =
  try
    let json = Yojson.Safe.from_string content in
    Ok (parse_lexicon_doc json)
  with
  | Yojson.Json_error e ->
      Error ("JSON parse error: " ^ e)
  | Failure e ->
      Error ("Parse error: " ^ e)
  | e ->
      Error ("Unexpected error: " ^ Printexc.to_string e)
