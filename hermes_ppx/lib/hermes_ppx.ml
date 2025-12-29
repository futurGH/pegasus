open Ppxlib

(* convert nsid to module path: "app.bsky.graph.get" -> ["App"; "Bsky"; "Graph"; "Get"] *)
let nsid_to_module_path nsid =
  String.split_on_char '.' nsid |> List.map String.capitalize_ascii

(* convert nsid to flat module name: "com.atproto.identity.resolveHandle" -> "Com_atproto_identity_resolveHandle" *)
let nsid_to_flat_module_name nsid =
  let flat = String.concat "_" (String.split_on_char '.' nsid) in
  String.capitalize_ascii flat

(* build module access expression from path: ["App"; "Bsky"] -> App.Bsky *)
let build_module_path ~loc path =
  match path with
  | [] ->
      Location.raise_errorf ~loc "Empty module path"
  | first :: rest ->
      List.fold_left
        (fun acc part ->
          let lid = Loc.make ~loc (Longident.Ldot (acc.txt, part)) in
          lid )
        (Loc.make ~loc (Longident.Lident first))
        rest

(* build full expression for flat module structure: Module_name.Main.call *)
let build_call_expr_flat ~loc nsid =
  let module_name = nsid_to_flat_module_name nsid in
  (* Build: Module_name.Main.call *)
  let lid = Longident.(Ldot (Ldot (Lident module_name, "Main"), "call")) in
  Ast_builder.Default.pexp_ident ~loc (Loc.make ~loc lid)

(* build full expression: Module.Path.call (nested style, kept for compatibility) *)
let build_call_expr ~loc nsid =
  let parts = nsid_to_module_path nsid in
  let module_lid = build_module_path ~loc parts in
  let call_lid = Loc.make ~loc (Longident.Ldot (module_lid.txt, "call")) in
  Ast_builder.Default.pexp_ident ~loc call_lid

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
  build_call_expr_flat ~loc nsid

let xrpc_extension =
  Extension.V3.declare "xrpc" Extension.Context.expression
    Ast_pattern.(pstr __)
    expand

let rule = Context_free.Rule.extension xrpc_extension

let () = Driver.register_transformation "hermes_ppx" ~rules:[rule]
