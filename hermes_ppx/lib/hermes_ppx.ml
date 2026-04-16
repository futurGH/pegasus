open Ppxlib

(* convert nsid to module path: "app.bsky.graph.get" -> ["App"; "Bsky"; "Graph"; "Get"] *)
let nsid_to_module_path nsid =
  String.split_on_char '.' nsid |> List.map String.capitalize_ascii

(* build full expression: Module.Name.Main.call *)
let build_call_expr ~loc nsid =
  let module_path = nsid_to_module_path nsid in
  let module_lid =
    match module_path with
    | [] ->
        Location.raise_errorf ~loc "Expected non-empty nsid"
    | hd :: tl ->
        List.fold_left
          (fun acc part -> Longident.Ldot (acc, part))
          (Longident.Lident hd) tl
  in
  let lid = Longident.(Ldot (Ldot (module_lid, "Main"), "call")) in
  Ast_builder.Default.pexp_ident ~loc (Loc.make ~loc lid)

(* parse method and nsid from structure items *)
let parse_method_and_nsid ~loc str =
  match str with
  | [{pstr_desc= Pstr_eval (expr, _); _}] -> (
    match expr.pexp_desc with
    (* [%xrpc get "nsid"] *)
    | Pexp_apply
        ( {pexp_desc= Pexp_ident {txt= Lident method_; _}; _}
        , [(Nolabel, {pexp_desc= Pexp_constant (Pconst_string (nsid, _, _)); _})]
        ) ->
        let method_lower = String.lowercase_ascii method_ in
        if method_lower = "get" || method_lower = "post" then
          (method_lower, nsid)
        else
          Location.raise_errorf ~loc "Expected 'get' or 'post', got '%s'"
            method_
    (* [%xrpc "nsid"] - assume get *)
    | Pexp_constant (Pconst_string (nsid, _, _)) ->
        ("get", nsid)
    | _ ->
        Location.raise_errorf ~loc
          "Expected [%%xrpc get \"nsid\"] or [%%xrpc post \"nsid\"]" )
  | _ ->
      Location.raise_errorf ~loc
        "Expected [%%xrpc get \"nsid\"] or [%%xrpc post \"nsid\"]"

let expand ~ctxt str =
  let loc = Expansion_context.Extension.extension_point_loc ctxt in
  let _method, nsid = parse_method_and_nsid ~loc str in
  build_call_expr ~loc nsid

let xrpc_extension =
  Extension.V3.declare "xrpc" Extension.Context.expression
    Ast_pattern.(pstr __)
    expand

let rule = Context_free.Rule.extension xrpc_extension

(* rewrite record types annotated with [@@deriving xrpc_query] by injecting
   Hermes_util [@of_yojson]/[@to_yojson] attrs on fields that need query string
   coercion, then swaps the deriving to [@@deriving yojson {strict = false}]. *)

let hermes_query name ~loc =
  Ast_builder.Default.pexp_ident ~loc
    (Loc.make ~loc (Longident.Ldot (Ldot (Lident "Hermes", "Query"), name)))

let make_attr ~loc name expr =
  { attr_name= Loc.make ~loc name
  ; attr_payload= PStr [Ast_builder.Default.pstr_eval ~loc expr []]
  ; attr_loc= loc }

(* classify a core_type and return attrs to inject *)
let query_attrs_for_type (ct : core_type) =
  let loc = ct.ptyp_loc in
  let of_ n = make_attr ~loc "of_yojson" (hermes_query n ~loc) in
  let to_ n = make_attr ~loc "to_yojson" (hermes_query n ~loc) in
  match ct.ptyp_desc with
  (* int *)
  | Ptyp_constr ({txt= Lident "int"; _}, []) ->
      [of_ "query_int_of_yojson"]
  (* bool *)
  | Ptyp_constr ({txt= Lident "bool"; _}, []) ->
      [of_ "query_bool_of_yojson"]
  (* T option -> inspect T *)
  | Ptyp_constr ({txt= Lident "option"; _}, [inner]) -> (
    match inner.ptyp_desc with
    | Ptyp_constr ({txt= Lident "int"; _}, []) ->
        [of_ "query_int_option_of_yojson"]
    | Ptyp_constr ({txt= Lident "bool"; _}, []) ->
        [of_ "query_bool_option_of_yojson"]
    (* T list option -> inspect T *)
    | Ptyp_constr ({txt= Lident "list"; _}, [list_inner]) -> (
      match list_inner.ptyp_desc with
      | Ptyp_constr ({txt= Lident "string"; _}, []) ->
          [ of_ "query_string_list_option_of_yojson"
          ; to_ "query_string_list_option_to_yojson" ]
      | Ptyp_constr ({txt= Lident "int"; _}, []) ->
          [ of_ "query_int_list_option_of_yojson"
          ; to_ "query_int_list_option_to_yojson" ]
      | _ ->
          [] )
    | _ ->
        [] )
  (* T list -> inspect T *)
  | Ptyp_constr ({txt= Lident "list"; _}, [inner]) -> (
    match inner.ptyp_desc with
    | Ptyp_constr ({txt= Lident "string"; _}, []) ->
        [of_ "query_string_list_of_yojson"; to_ "query_string_list_to_yojson"]
    | Ptyp_constr ({txt= Lident "int"; _}, []) ->
        [of_ "query_int_list_of_yojson"; to_ "query_int_list_to_yojson"]
    | _ ->
        [] )
  | _ ->
      []

let transform_label_decl (ld : label_declaration) : label_declaration =
  let extra_attrs = query_attrs_for_type ld.pld_type in
  {ld with pld_attributes= ld.pld_attributes @ extra_attrs}

(* build the [@@deriving yojson {strict = false}] attribute *)
let yojson_deriving_attr ~loc =
  let strict_false =
    ( Loc.make ~loc (Lident "strict")
    , Ast_builder.Default.pexp_construct ~loc
        (Loc.make ~loc (Lident "false"))
        None )
  in
  let yojson_expr =
    Ast_builder.Default.pexp_apply ~loc
      (Ast_builder.Default.pexp_ident ~loc (Loc.make ~loc (Lident "yojson")))
      [(Nolabel, Ast_builder.Default.pexp_record ~loc [strict_false] None)]
  in
  { attr_name= Loc.make ~loc "deriving"
  ; attr_payload= PStr [Ast_builder.Default.pstr_eval ~loc yojson_expr []]
  ; attr_loc= loc }

let is_xrpc_query (attr : attribute) = attr.attr_name.txt = "xrpc_query"

let transform_type_decl (td : type_declaration) =
  let has_xrpc_query = List.exists is_xrpc_query td.ptype_attributes in
  if not has_xrpc_query then td
  else
    let kind =
      match td.ptype_kind with
      | Ptype_record fields ->
          Ptype_record (List.map transform_label_decl fields)
      | other ->
          other
    in
    let attrs =
      List.map
        (fun attr ->
          if is_xrpc_query attr then yojson_deriving_attr ~loc:attr.attr_loc
          else attr )
        td.ptype_attributes
    in
    {td with ptype_kind= kind; ptype_attributes= attrs}

let rec transform_structure str =
  List.map
    (fun (si : structure_item) ->
      match si.pstr_desc with
      | Pstr_type (rf, tds) ->
          {si with pstr_desc= Pstr_type (rf, List.map transform_type_decl tds)}
      | Pstr_module mb ->
          { si with
            pstr_desc=
              Pstr_module {mb with pmb_expr= transform_module_expr mb.pmb_expr}
          }
      | Pstr_recmodule mbs ->
          { si with
            pstr_desc=
              Pstr_recmodule
                (List.map
                   (fun mb ->
                     {mb with pmb_expr= transform_module_expr mb.pmb_expr} )
                   mbs ) }
      | _ ->
          si )
    str

and transform_module_expr (me : module_expr) =
  match me.pmod_desc with
  | Pmod_structure str ->
      {me with pmod_desc= Pmod_structure (transform_structure str)}
  | _ ->
      me

let () =
  Driver.register_transformation "hermes_ppx" ~rules:[rule]
    ~instrument:
      (Driver.Instrument.V2.make
         (fun _ctx str -> transform_structure str)
         ~position:Before )
