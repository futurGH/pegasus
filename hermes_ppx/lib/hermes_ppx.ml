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

let () = Driver.register_transformation "hermes_ppx" ~rules:[rule]
