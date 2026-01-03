open Hermes_cli

let dune_file =
  Printf.sprintf
    {|(library
 (name %s)
 (libraries hermes yojson lwt)
 (preprocess (pps ppx_deriving_yojson)))|}

let util_file =
  Printf.sprintf
    {|let query_string_list_of_yojson = function
  | `List l ->
      Ok (List.filter_map (function `String s -> Some s | _ -> None) l)
  | `String s ->
      Ok [s]
  | `Null ->
      Ok []
  | _ ->
      Error "expected string or string list"

let query_string_list_to_yojson l = `List (List.map (fun s -> `String s) l)

let query_int_list_of_yojson = function
  | `List l ->
      Ok (List.filter_map (function `Int i -> Some i | _ -> None) l)
  | `Int i ->
      Ok [i]
  | `Null ->
      Ok []
  | _ ->
      Error "expected int or int list"

let query_int_list_to_yojson l = `List (List.map (fun i -> `Int i) l)

let query_string_list_option_of_yojson = function
  | `List l ->
      Ok (Some (List.filter_map (function `String s -> Some s | _ -> None) l))
  | `String s ->
      Ok (Some [s])
  | `Null ->
      Ok None
  | _ ->
      Error "expected string or string list"

let query_string_list_option_to_yojson = function
  | Some l ->
      `List (List.map (fun s -> `String s) l)
  | None ->
      `Null

let query_int_list_option_of_yojson = function
  | `List l ->
      Ok (Some (List.filter_map (function `Int i -> Some i | _ -> None) l))
  | `Int i ->
      Ok (Some [i])
  | `Null ->
      Ok None
  | _ ->
      Error "expected int or int list"

let query_int_list_option_to_yojson = function
  | Some l ->
      `List (List.map (fun i -> `Int i) l)
  | None ->
      `Null|}

(* recursively find all json files in a path (file or directory) *)
let find_json_files path =
  let rec aux acc p =
    if Sys.is_directory p then
      Sys.readdir p |> Array.to_list
      |> List.map (Filename.concat p)
      |> List.fold_left aux acc
    else if Filename.check_suffix p ".json" then p :: acc
    else acc
  in
  aux [] path

let generate_index lexicons =
  let nsids = List.map (fun lexicon -> lexicon.Lexicon_types.id) lexicons in
  let trie = Naming.group_nsids_by_prefix nsids in
  let rec build_index (trie : Naming.trie) index indent =
    match trie with
    | Node children ->
        List.fold_left
          (fun acc (key, child) ->
            match (child : Naming.trie) with
            | Module nsid ->
                let module_name = Naming.flat_module_name_of_nsid nsid in
                acc ^ indent
                ^ Printf.sprintf "module %s = %s\n"
                    (String.capitalize_ascii key)
                    module_name
            | Node _ ->
                acc ^ indent
                ^ Printf.sprintf "module %s = struct\n"
                    (String.capitalize_ascii key)
                ^ build_index child index (indent ^ "  ")
                ^ indent ^ "end\n" )
          index children
    | _ ->
        failwith "build_index called with invalid trie"
  in
  build_index (Node trie) "" ""

(* generate module structure from lexicons *)
let generate ~inputs ~output_dir ~module_name =
  (* create output directory *)
  if not (Sys.file_exists output_dir) then Sys.mkdir output_dir 0o755 ;
  (* find all lexicon files from all inputs *)
  let files = List.concat_map find_json_files inputs in
  Printf.printf "Found %d lexicon files\n" (List.length files) ;
  (* parse all files *)
  let lexicons =
    List.filter_map
      (fun path ->
        match Parser.parse_file path with
        | Ok doc ->
            Printf.printf "  Parsed: %s\n" doc.Lexicon_types.id ;
            Some doc
        | Error e ->
            Printf.eprintf "  Error parsing %s: %s\n" path e ;
            None )
      files
  in
  Printf.printf "Successfully parsed %d lexicons\n" (List.length lexicons) ;
  (* find file-level SCCs to detect cross-file cycles *)
  let sccs = Scc.find_file_sccs lexicons in
  Printf.printf "Found %d file-level SCCs\n" (List.length sccs) ;
  (* track shared module index for unique naming *)
  let shared_index = ref 0 in
  (* generate each SCC *)
  List.iter
    (fun scc ->
      match scc with
      | [] ->
          ()
      | [doc] ->
          (* single file, no cycle - generate normally *)
          let code = Codegen.gen_lexicon_module doc in
          let rel_path = Naming.file_path_of_nsid doc.Lexicon_types.id in
          let full_path = Filename.concat output_dir rel_path in
          let oc = open_out full_path in
          output_string oc code ;
          close_out oc ;
          Printf.printf "  Generated: %s\n" rel_path
      | docs ->
          (* multiple files forming a cycle - use shared module strategy *)
          incr shared_index ;
          let nsids = List.map (fun d -> d.Lexicon_types.id) docs in
          Printf.printf "  Cyclic lexicons: %s\n" (String.concat ", " nsids) ;
          (* sort for consistent ordering *)
          let sorted_docs =
            List.sort
              (fun a b -> String.compare a.Lexicon_types.id b.Lexicon_types.id)
              docs
          in
          (* generate shared module with all types *)
          let shared_module_name =
            Naming.shared_module_name nsids !shared_index
          in
          let shared_file = Naming.shared_file_name nsids !shared_index in
          let code = Codegen.gen_shared_module sorted_docs in
          let full_path = Filename.concat output_dir shared_file in
          let oc = open_out full_path in
          output_string oc code ;
          close_out oc ;
          Printf.printf "  Generated shared: %s\n" shared_file ;
          (* generate re-export modules for each nsid *)
          List.iter
            (fun doc ->
              let stub =
                Codegen.gen_reexport_module ~shared_module_name
                  ~all_merged_docs:sorted_docs doc
              in
              let rel_path = Naming.file_path_of_nsid doc.Lexicon_types.id in
              let full_path = Filename.concat output_dir rel_path in
              let oc = open_out full_path in
              output_string oc stub ;
              close_out oc ;
              Printf.printf "  Generated: %s -> %s\n" rel_path
                shared_module_name )
            docs )
    sccs ;
  (* generate index file *)
  let index_path =
    Filename.concat output_dir (String.lowercase_ascii module_name ^ ".ml")
  in
  let oc = open_out index_path in
  Printf.fprintf oc "(* %s - generated from atproto lexicons *)\n\n" module_name ;
  (* export each lexicon as a module alias *)
  Out_channel.output_string oc (generate_index lexicons) ;
  close_out oc ;
  Printf.printf "Generated index: %s\n" index_path ;
  (* generate dune file *)
  let dune_path = Filename.concat output_dir "dune" in
  let oc = open_out dune_path in
  Out_channel.output_string oc (dune_file (String.lowercase_ascii module_name)) ;
  close_out oc ;
  Printf.printf "Generated dune file\n" ;
  (* generate util file *)
  let util_path = Filename.concat output_dir "hermes_util.ml" in
  let oc = open_out util_path in
  Printf.fprintf oc "(* %s - generated from atproto lexicons *)\n\n" module_name ;
  (* export each lexicon as a module alias *)
  Out_channel.output_string oc util_file ;
  close_out oc ;
  Printf.printf "Generated util file: %s\n" util_path ;
  Printf.printf "Done! Generated %d modules\n" (List.length lexicons)

let inputs =
  let doc = "lexicon files or directories to search recursively for JSON" in
  Cmdliner.Arg.(non_empty & pos_all file [] & info [] ~docv:"INPUT" ~doc)

let output_dir =
  let doc = "output directory for generated code" in
  Cmdliner.Arg.(
    required & opt (some string) None & info ["o"; "output"] ~docv:"DIR" ~doc )

let module_name =
  let doc = "name of the generated module" in
  Cmdliner.Arg.(
    value
    & opt string "Hermes_lexicons"
    & info ["m"; "module-name"] ~docv:"NAME" ~doc )

let generate_cmd =
  let doc = "generate ocaml types from atproto lexicons" in
  let info = Cmdliner.Cmd.info "generate" ~doc in
  let generate' inputs output_dir module_name =
    generate ~inputs ~output_dir ~module_name
  in
  Cmdliner.Cmd.v info
    Cmdliner.Term.(const generate' $ inputs $ output_dir $ module_name)

let main_cmd =
  let doc = "hermes - atproto lexicon code generator" in
  let info = Cmdliner.Cmd.info "hermes-cli" ~version:"0.1.0" ~doc in
  Cmdliner.Cmd.group info [generate_cmd]

let () = exit (Cmdliner.Cmd.eval main_cmd)
